import pytest
import sqlalchemy
from funcy import filter, first
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

create_second_view = """
CREATE VIEW VIEW_TWO AS
SELECT t.code, v.title
FROM TABLE_ONE t, VIEW_ONE v
"""

create_schema = """
CREATE SCHEMA IF NOT EXISTS other_schema;
"""

create_view_three = """
CREATE VIEW other_schema.VIEW_THREE AS
SELECT *
FROM TABLE_ONE
"""

create_view_four = """
CREATE VIEW VIEW_FOUR AS
SELECT v1.code, v3.title
FROM VIEW_ONE v1, other_schema.VIEW_THREE v3
"""

create_materialized_view = """
CREATE MATERIALIZED VIEW materialized_view AS
SELECT *
FROM TABLE_ONE
"""

from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin


@pytest.mark.integration
def test_postgres():
    with PostgresContainer(
        "postgres:14.7", password="postgres", user="postgres"
    ) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.connect() as connection:
            connection.exec_driver_sql(create_enum)
            connection.exec_driver_sql(create_tables)
            connection.exec_driver_sql(create_view)
            connection.exec_driver_sql(create_second_view)
            connection.exec_driver_sql(create_schema)
            connection.exec_driver_sql(create_view_three)
            connection.exec_driver_sql(create_view_four)
            connection.exec_driver_sql(create_materialized_view)

        config = PostgreSQLPlugin(
            type="postgresql",
            name="test",
            database="test",
            password=SecretStr("postgres"),
            user="postgres",
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
        )

        data_entities = Adapter(config).get_data_entity_list()
        database_services: list[DataEntity] = find_by_type(
            data_entities, DataEntityType.DATABASE_SERVICE
        )
        assert len(database_services) == 1
        database_service = database_services[0]
        assert len(database_service.data_entity_group.entities_list) == 6

        tables = find_by_type(data_entities, DataEntityType.TABLE)
        assert len(tables) == 1
        table = tables[0]
        assert len(table.dataset.field_list) == 7

        views = find_by_type(data_entities, DataEntityType.VIEW)
        assert len(views) == 5
        view_one = first(filter(lambda x: x.name == "view_one", views))
        assert len(view_one.dataset.field_list) == 7

        view_two = first(filter(lambda x: x.name == "view_two", views))
        assert len(view_two.dataset.field_list) == 2
        depends = view_two.data_transformer.inputs
        assert len(depends) == 2

        assert table.oddrn in depends
        assert view_one.oddrn in depends

        view_three = first(filter(lambda x: x.name == "view_three", views))
        depends = view_three.data_transformer.inputs
        assert table.oddrn in depends

        view_four = first(filter(lambda x: x.name == "view_four", views))
        depends = view_four.data_transformer.inputs
        assert view_one.oddrn in depends
        assert view_three.oddrn in depends

        mat_view = first(filter(lambda x: x.name == "materialized_view", views))
        depends = mat_view.data_transformer.inputs
        assert len(mat_view.dataset.field_list) == 7
        assert table.oddrn in depends

        assert data_entities.json()
