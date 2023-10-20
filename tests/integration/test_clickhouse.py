import pytest
from clickhouse_driver import Client
from funcy import lfilter
from odd_models import DataEntity
from odd_models.models import DataEntityList, DataEntityType
from pydantic import SecretStr
from testcontainers.clickhouse import ClickHouseContainer

from odd_collector.adapters.clickhouse.adapter import Adapter
from odd_collector.domain.plugin import ClickhousePlugin


def find_by_type(
    data_entity_list: DataEntityList, data_entity_type: DataEntityType
) -> list[DataEntity]:
    """Find data entities by type."""
    return lfilter(
        lambda data_entity: data_entity.type == data_entity_type, data_entity_list.items
    )


create_databse = "CREATE DATABASE IF NOT EXISTS my_database"
create_table = """
CREATE TABLE my_database.test (
    a Date,
    b UInt64, 
    c Nested(d String, e String, f Nested(g String)),
    d Date32,
    e DateTime('UTC'),
    f DateTime64(3, 'UTC'),
    c_uuid UUID,
    c_agg AggregateFunction(uniq, UInt64),
    c_low_cardinality LowCardinality(String)
) ENGINE = MergeTree ORDER BY (a, b)
"""


@pytest.mark.integration
def test_clickhouse():
    with ClickHouseContainer(
        user="username",
        password="password",
    ).with_bind_ports(8123, 8123) as clickhouse:
        clickhouse.with_env("CLICKHOUSE_DB", "my_database")

        client = Client.from_url(
            clickhouse.get_connection_url(),
        )
        client.execute(create_databse)

        client.execute(create_table)

        config = ClickhousePlugin(
            type="clickhouse",
            name="clickhouse_adapter",
            host=clickhouse.get_container_host_ip(),
            port=8123,
            database="my_database",
            user="username",
            password=SecretStr("password"),
        )

        data_entities = Adapter(config).get_data_entity_list()

        database_services: list[DataEntity] = find_by_type(
            data_entities, DataEntityType.DATABASE_SERVICE
        )

        assert len(database_services) == 1
        database_service = database_services[0]
        assert len(database_service.data_entity_group.entities_list) == 1

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
        table = tables[0]
        assert len(table.dataset.field_list) == 13

        assert data_entities.json()
