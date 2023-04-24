import pytest
import sqlalchemy
from odd_models import DataEntity
from odd_models.models import DataEntityType
from pydantic import SecretStr
from testcontainers.mssql import SqlServerContainer

from tests.integration.helpers import find_by_type

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

from odd_collector.adapters.mssql.adapter import Adapter
from odd_collector.domain.plugin import MSSQLPlugin


@pytest.mark.integration
def test_mssql():
    with SqlServerContainer(
        image="mcr.microsoft.com/azure-sql-edge",
        port=1433,
        password="yourStrong(!)Password",
    ) as mssql:
        engine = sqlalchemy.create_engine(mssql.get_connection_url())

        with engine.connect() as connection:
            connection.exec_driver_sql(create_tables)
            connection.exec_driver_sql(create_view)

        config = MSSQLPlugin(
            type="mssql",
            name="test_mysql",
            database="tempdb",
            password=SecretStr("yourStrong(!)Password"),
            user="SA",
            host=mssql.get_container_host_ip(),
            port=mssql.get_exposed_port(1433),
        )

        data_entities = Adapter(config).get_data_entity_list()
        database_services: list[DataEntity] = find_by_type(
            data_entities, DataEntityType.DATABASE_SERVICE
        )
        print(data_entities)

        assert len(database_services) == 2  # 1 for the database, 1 for the schema

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
        table = tables[0]
        assert len(table.dataset.field_list) == 5

        views = find_by_type(data_entities, DataEntityType.VIEW)
        assert len(views) == 1
        view = views[0]
        assert len(view.dataset.field_list) == 2
        assert len(view.data_transformer.inputs) == 1
        assert view.data_transformer.inputs[0] == table.oddrn
