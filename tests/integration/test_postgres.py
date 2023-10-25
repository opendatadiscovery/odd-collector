import odd_models
import pytest
import sqlalchemy
from pydantic import SecretStr
from testcontainers.postgres import PostgresContainer

from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin
from tests.integration.helpers import find_by_name, find_by_type


def create_primary_schema(connection: sqlalchemy.engine.Connection):
    create_enum = """CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"""
    create_tables = """CREATE TABLE IF NOT EXISTS TABLE_ONE (
        code        char(5) CONSTRAINT firstkey PRIMARY KEY,
        title       varchar(40) NOT NULL,
        did         integer NOT NULL,
        date_prod   date,
        kind        varchar(10),
        len         interval hour to minute,
        status      mood
    )"""

    create_view = """
    CREATE VIEW VIEW_ONE AS
    SELECT *
    FROM TABLE_ONE
    """

    create_second_view = """
    CREATE VIEW VIEW_TWO AS
    SELECT t.code, v.title
    FROM TABLE_ONE t, VIEW_ONE v
    """

    connection.exec_driver_sql(create_enum)
    connection.exec_driver_sql(create_tables)
    connection.exec_driver_sql(create_view)
    connection.exec_driver_sql(create_second_view)


def create_other_schema(connection: sqlalchemy.engine.Connection):
    create_schema = """
    CREATE SCHEMA IF NOT EXISTS other_schema;
    """

    create_view_three = """
    CREATE VIEW other_schema.VIEW_THREE AS
    SELECT *
    FROM TABLE_ONE
    """

    create_view_four = """
    CREATE VIEW other_schema.VIEW_FOUR AS
    SELECT v1.code, v3.title
    FROM VIEW_ONE v1, other_schema.VIEW_THREE v3
    """

    create_materialized_view = """
    CREATE MATERIALIZED VIEW other_schema.materialized_view AS
    SELECT *
    FROM TABLE_ONE
    """

    connection.exec_driver_sql(create_schema)
    connection.exec_driver_sql(create_view_three)
    connection.exec_driver_sql(create_view_four)
    connection.exec_driver_sql(create_materialized_view)


@pytest.fixture(scope="module")
def data_entity_list() -> odd_models.DataEntityList:
    with PostgresContainer(
        "postgres:14.7", password="postgres", user="postgres"
    ) as container:
        engine = sqlalchemy.create_engine(container.get_connection_url())

        with engine.connect() as connection:
            create_primary_schema(connection)
            create_other_schema(connection)

        config = PostgreSQLPlugin(
            type="postgresql",
            name="test",
            database="test",
            password=SecretStr("postgres"),
            user="postgres",
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
        )

        return Adapter(config).get_data_entity_list()


def test_decoding_to_json(data_entity_list: odd_models.DataEntityList):
    assert data_entity_list.json()


def test_data_base_service(data_entity_list: odd_models.DataEntityList):
    database_services: list[odd_models.DataEntity] = find_by_type(
        data_entity_list, odd_models.DataEntityType.DATABASE_SERVICE
    )
    assert len(database_services) == 3

    database = find_by_name(data_entity_list, "test")
    public_schema = find_by_name(data_entity_list, "public")
    other_schema = find_by_name(data_entity_list, "other_schema")

    assert database is not None
    assert public_schema is not None
    assert other_schema is not None

    assert database.data_entity_group is not None
    assert len(database.data_entity_group.entities_list) == 2
    assert public_schema.oddrn in database.data_entity_group.entities_list
    assert other_schema.oddrn in database.data_entity_group.entities_list


def test_public_schema(data_entity_list: odd_models.DataEntityList):
    public_schema = find_by_name(data_entity_list, "public")
    assert public_schema is not None
    assert public_schema.data_entity_group is not None

    table_one = find_by_name(data_entity_list, "table_one")
    view_one = find_by_name(data_entity_list, "view_one")
    view_two = find_by_name(data_entity_list, "view_two")

    assert len(public_schema.data_entity_group.entities_list) == 3

    for data_entity in [table_one, view_one, view_two]:
        assert data_entity is not None
        assert data_entity.oddrn in public_schema.data_entity_group.entities_list


def test_other_schema(data_entity_list: odd_models.DataEntityList):
    other_schema = find_by_name(data_entity_list, "other_schema")
    assert other_schema is not None
    assert other_schema.data_entity_group is not None
    assert len(other_schema.data_entity_group.entities_list) == 3

    view_three = find_by_name(data_entity_list, "view_three")
    view_four = find_by_name(data_entity_list, "view_four")
    materialized_view = find_by_name(data_entity_list, "materialized_view")

    for data_entity in [view_three, view_four, materialized_view]:
        assert data_entity is not None
        assert data_entity.oddrn in other_schema.data_entity_group.entities_list


def test_table_one(data_entity_list: odd_models.DataEntityList):
    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one is not None
    assert table_one.dataset is not None
    assert table_one.dataset.field_list is not None
    assert len(table_one.dataset.field_list) == 7


def test_view_one(data_entity_list: odd_models.DataEntityList):
    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one is not None
    assert view_one.dataset is not None
    assert view_one.dataset.field_list is not None
    assert len(view_one.dataset.field_list) == 7

    assert view_one.data_transformer is not None
    assert view_one.data_transformer.inputs is not None

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_one.data_transformer.inputs


def test_view_two(data_entity_list: odd_models.DataEntityList):
    view_two = find_by_name(data_entity_list, "view_two")
    assert view_two is not None
    assert view_two.dataset is not None
    assert view_two.dataset.field_list is not None
    assert len(view_two.dataset.field_list) == 2

    assert view_two.data_transformer is not None
    assert view_two.data_transformer.inputs is not None
    assert len(view_two.data_transformer.inputs) == 2

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_two.data_transformer.inputs
    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one.oddrn in view_two.data_transformer.inputs


def test_view_three(data_entity_list: odd_models.DataEntityList):
    view_three = find_by_name(data_entity_list, "view_three")
    assert view_three is not None
    assert view_three.dataset is not None
    assert view_three.dataset.field_list is not None
    assert len(view_three.dataset.field_list) == 7

    assert view_three.data_transformer is not None
    assert view_three.data_transformer.inputs is not None
    assert len(view_three.data_transformer.inputs) == 1

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_three.data_transformer.inputs


def test_view_four(data_entity_list: odd_models.DataEntityList):
    view_four = find_by_name(data_entity_list, "view_four")
    assert view_four is not None
    assert view_four.dataset is not None
    assert view_four.dataset.field_list is not None
    assert len(view_four.dataset.field_list) == 2

    assert view_four.data_transformer is not None
    assert view_four.data_transformer.inputs is not None
    assert len(view_four.data_transformer.inputs) == 2

    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one.oddrn in view_four.data_transformer.inputs

    view_three = find_by_name(data_entity_list, "view_three")
    assert view_three.oddrn in view_four.data_transformer.inputs


def test_materialized_view(data_entity_list: odd_models.DataEntityList):
    materialized_view = find_by_name(data_entity_list, "materialized_view")
    assert materialized_view is not None
    assert materialized_view.dataset is not None
    assert materialized_view.dataset.field_list is not None
    assert len(materialized_view.dataset.field_list) == 7

    assert materialized_view.data_transformer is not None
    assert materialized_view.data_transformer.inputs is not None
    assert len(materialized_view.data_transformer.inputs) == 1

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in materialized_view.data_transformer.inputs
