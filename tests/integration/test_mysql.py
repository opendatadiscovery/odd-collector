import odd_models
import pytest
import sqlalchemy
from pydantic import SecretStr
from testcontainers.mysql import MySqlContainer

from odd_collector.adapters.mysql.adapter import Adapter
from odd_collector.domain.plugin import MySQLPlugin
from tests.integration.helpers import find_by_name, find_by_type


def create_primary_schema(connection: sqlalchemy.engine.Connection):
    create_tables = """
    CREATE TABLE Persons (
        PersonID int,
        LastName varchar(255),
        FirstName varchar(255),
        Address varchar(255),
        City varchar(255)
    );"""

    create_view = """
        CREATE VIEW persons_names AS
        SELECT LastName, FirstName
        FROM Persons;
    """

    create_view_from_view = """
        CREATE VIEW persons_last_names AS
        SELECT LastName
        FROM persons_names;
    """

    connection.exec_driver_sql(create_tables)
    connection.exec_driver_sql(create_view)
    connection.exec_driver_sql(create_view_from_view)


def create_other_schema(connection: sqlalchemy.engine.Connection):
    create_other_schema = """
        CREATE DATABASE `other_schema`;
    """

    create_tables = """
        CREATE TABLE `other_schema`.`Persons` (
            PersonID int,
            LastName varchar(255),
            FirstName varchar(255),
            Address varchar(255),
            City varchar(255)
        );"""

    create_view = """
        CREATE VIEW `other_schema`.`persons_names` AS
        SELECT LastName, FirstName
        FROM `other_schema`.`Persons`;
    """

    create_view_from_view = """
        CREATE VIEW `other_schema`.`persons_last_names` AS
        SELECT LastName
        FROM `other_schema`.`persons_names`;
    """

    connection.exec_driver_sql(create_other_schema)
    connection.exec_driver_sql(create_tables)
    connection.exec_driver_sql(create_view)
    connection.exec_driver_sql(create_view_from_view)


def entities_are_unique(entities: list[odd_models.DataEntity]):
    return len(entities) == len({e.oddrn for e in entities})


@pytest.fixture(scope="module")
def data_entities() -> odd_models.DataEntityList:
    with MySqlContainer(MYSQL_USER="root") as mysql:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())

        with engine.connect() as connection:
            create_primary_schema(connection)
            create_other_schema(connection)

        config = MySQLPlugin(
            type="mysql",
            name="test_mysql",
            database="test",
            password=SecretStr("test"),
            user="root",
            host=mysql.get_container_host_ip(),
            port=int(mysql.get_exposed_port(3306)),
        )

        return Adapter(config).get_data_entity_list()


def test_entities_are_unique(data_entities: odd_models.DataEntityList):
    assert entities_are_unique(data_entities.items)


def test_fetch_one_database_from_config(data_entities: odd_models.DataEntityList):
    databases: list[odd_models.DataEntity] = find_by_type(
        data_entities, odd_models.DataEntityType.DATABASE_SERVICE
    )
    assert len(databases) == 1
    database = databases[0]
    assert database.data_entity_group is not None
    assert len(database.data_entity_group.entities_list) == 3

    entities = database.data_entity_group.entities_list
    assert len(entities) == len(set(entities))


def test_fetch_only_one_table(data_entities: odd_models.DataEntityList):
    tables = find_by_type(data_entities, odd_models.DataEntityType.TABLE)

    assert entities_are_unique(tables)

    table = tables[0]

    assert len(tables) == 1
    assert table.dataset is not None
    assert len(table.dataset.field_list) == 5
    assert entities_are_unique(table.dataset.field_list)


def test_fetch_two_views(data_entities: odd_models.DataEntityList):
    views = find_by_type(data_entities, odd_models.DataEntityType.VIEW)
    assert len(views) == 2
    assert entities_are_unique(views)


def test_view_depends_on_table(data_entities: odd_models.DataEntityList):
    table_entity = find_by_name(data_entities, "Persons")
    entity = find_by_name(data_entities, "persons_names")

    assert len(entity.dataset.field_list) == 2
    assert len(entity.data_transformer.inputs) == 1
    assert entity.data_transformer.inputs[0] == table_entity.oddrn


def test_view_depends_on_view(data_entities: odd_models.DataEntityList):
    view_entity = find_by_name(data_entities, "persons_names")
    entity = find_by_name(data_entities, "persons_last_names")
    assert len(entity.data_transformer.inputs) == 1
    assert entity.data_transformer.inputs[0] == view_entity.oddrn


def test_decoding_data_entities(data_entities: odd_models.DataEntityList):
    assert data_entities.json()
