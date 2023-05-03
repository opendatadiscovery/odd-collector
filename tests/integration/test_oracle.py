import pytest
import sqlalchemy
from odd_models import DataEntity
from odd_models.models import DataEntityType
from pydantic import SecretStr
from testcontainers.oracle import OracleDbContainer


from odd_collector.domain.plugin import OraclePlugin
from tests.integration.helpers import find_by_type


create_user = """
    CREATE USER test_user
    IDENTIFIED BY oracle
    DEFAULT TABLESPACE users
"""
grant_access = "GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, UNLIMITED TABLESPACE, CREATE PROCEDURE TO test_user"
alter_session = "ALTER SESSION SET CURRENT_SCHEMA = test_user"
create_tables = """
    CREATE TABLE test_table (
    person_id NUMBER,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    PRIMARY KEY(person_id)
)
"""
create_view = """
    CREATE VIEW test_view AS
    SELECT *
    FROM test_table
"""


@pytest.mark.integration
def test_oracle():
    with OracleDbContainer() as oracle:
        engine = sqlalchemy.create_engine(oracle.get_connection_url())

        with engine.begin() as connection:
            connection.execute(sqlalchemy.text(create_user))
            connection.execute(sqlalchemy.text(grant_access))
            connection.execute(sqlalchemy.text(alter_session))
            connection.execute(sqlalchemy.text(create_tables))
            connection.execute(sqlalchemy.text(create_view))

        config = OraclePlugin(
            type="oracle",
            name="oracle",
            service="xe",
            password=SecretStr("oracle"),
            user="test_user",
            host=oracle.get_container_host_ip(),
            port=oracle.get_exposed_port(1521),
            thick_mode=True,
        )

        # import here because of patched imports in sqlalchemy_repository module
        from odd_collector.adapters.oracle.adapter import Adapter

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
        assert len(table.dataset.field_list) == 3
