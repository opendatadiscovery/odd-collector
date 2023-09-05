import pytest
import sqlalchemy
from odd_models import DataEntity
from odd_models.models import DataEntityType
from pydantic import SecretStr
from testcontainers.mysql import MySqlContainer

from tests.integration.helpers import find_by_name, find_by_type

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
FROM Persons
WHERE City = 'Sandnes';
"""

create_view_from_view = """
CREATE VIEW persons_last_names AS
SELECT LastName
FROM persons_names;
"""

from odd_collector.adapters.mysql.adapter import Adapter
from odd_collector.domain.plugin import MySQLPlugin


@pytest.mark.integration
def test_mysql():
    with MySqlContainer() as mysql:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())

        with engine.connect() as connection:
            connection.exec_driver_sql(create_tables)
            connection.exec_driver_sql(create_view)
            connection.exec_driver_sql(create_view_from_view)

        config = MySQLPlugin(
            type="mysql",
            name="test_mysql",
            database="test",
            password=SecretStr("test"),
            user="test",
            host=mysql.get_container_host_ip(),
            port=mysql.get_exposed_port(3306),
        )

        data_entities = Adapter(config).get_data_entity_list()
        database_services: list[DataEntity] = find_by_type(
            data_entities, DataEntityType.DATABASE_SERVICE
        )
        assert len(database_services) == 1
        database_service = database_services[0]
        assert len(database_service.data_entity_group.entities_list) == 3

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
        table = tables[0]
        assert len(table.dataset.field_list) == 5

        views = find_by_type(data_entities, DataEntityType.VIEW)
        assert len(views) == 2

        persons_view = find_by_name(data_entities, "persons_names")
        assert len(persons_view.dataset.field_list) == 2
        assert len(persons_view.data_transformer.inputs) == 1
        assert persons_view.data_transformer.inputs[0] == table.oddrn

        last_names_view = find_by_name(data_entities, "persons_last_names")
        assert len(last_names_view.dataset.field_list) == 1
        assert len(last_names_view.data_transformer.inputs) == 1
        assert last_names_view.data_transformer.inputs[0] == persons_view.oddrn

        assert data_entities.json()
