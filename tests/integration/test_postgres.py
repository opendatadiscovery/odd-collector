import pytest
import sqlalchemy
from odd_models.models import DataEntityType
from testcontainers.postgres import PostgresContainer

from tests.integration.helpers import find_by_type

create_tables = """CREATE TABLE IF NOT EXISTS TABLE_ONE (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
)"""


from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin


@pytest.mark.integration
def test_postgres():
    with PostgresContainer(
        "postgres:14.7", password="postgres", user="postgres"
    ) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.connect() as connection:
            connection.exec_driver_sql(create_tables)

        config = PostgreSQLPlugin(
            type="postgresql",
            name="test",
            database="test",
            password="postgres",
            user="postgres",
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
        )

        data_entities = Adapter(config).get_data_entity_list()
        database_services = find_by_type(data_entities, DataEntityType.DATABASE_SERVICE)
        assert len(database_services) == 1

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
