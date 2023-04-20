import pytest
import sqlalchemy
from odd_models import DataEntity
from odd_models.models import DataEntityType
from pydantic import SecretStr
from testcontainers.postgres import PostgresContainer

from tests.integration.helpers import find_by_type

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

from odd_collector.adapters.cockroachdb.adapter import Adapter
from odd_collector.domain.plugin import CockroachDBPlugin


@pytest.mark.integration
def test_cockroach():
    with PostgresContainer(
        "postgres:14.7", password="postgres", user="postgres"
    ) as cockroach:
        engine = sqlalchemy.create_engine(cockroach.get_connection_url())

        with engine.connect() as connection:
            connection.exec_driver_sql(create_enum)
            connection.exec_driver_sql(create_tables)
            connection.exec_driver_sql(create_view)

        config = CockroachDBPlugin(
            type="cockroachdb",
            name="test",
            database="test",
            password=SecretStr("postgres"),
            user="postgres",
            host=cockroach.get_container_host_ip(),
            port=cockroach.get_exposed_port(5432),
        )

        data_entities = Adapter(config).get_data_entity_list()
        database_services: list[DataEntity] = find_by_type(
            data_entities, DataEntityType.DATABASE_SERVICE
        )
        assert len(database_services) == 1
        database_service = database_services[0]
        assert len(database_service.data_entity_group.entities_list) == 2

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
        table = tables[0]
        assert len(table.dataset.field_list) == 7
