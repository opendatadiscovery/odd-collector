import datetime
from typing import List, Optional, Type

import pytest
from funcy import filter, first, lfilter
from odd_models.models import DataEntity, DataEntityType
from pydantic import SecretStr

from odd_collector.adapters.snowflake.adapter import Adapter
from odd_collector.adapters.snowflake.client import SnowflakeClientBase
from odd_collector.adapters.snowflake.domain import Column, Connection, Table, View
from odd_collector.adapters.snowflake.domain.pipe import RawPipe, RawStage
from odd_collector.domain.plugin import SnowflakePlugin

DATABASE_NAME = "TEST_DB"
SCHEMA = "PUBLIC"
TABLE_NAME = "TEST_TABLE"
FIRST_VIEW = "FIRST_VIEW"
SECOND_VIEW = "SECOND_VIEW"


class TestClient(SnowflakeClientBase):
    def get_raw_pipes(self) -> List[RawPipe]:
        return []

    def get_raw_stages(self) -> List[RawStage]:
        return []

    def get_tables(self) -> List[Table]:
        tables = [
            Table(
                upstream=[],
                downstream=[
                    Connection(
                        table_catalog=DATABASE_NAME,
                        table_name=FIRST_VIEW,
                        table_schema=SCHEMA,
                        domain="VIEW",
                    )
                ],
                table_catalog=DATABASE_NAME,
                table_schema=SCHEMA,
                table_name=TABLE_NAME,
                table_owner="ACCOUNTADMIN",
                table_type="BASE TABLE",
                is_transient="NO",
                clustering_key=None,
                row_count=0,
                retention_time=1,
                created=datetime.datetime.now(),
                last_altered=datetime.datetime.now(),
                table_comment=None,
                self_referencing_column_name=None,
                reference_generation=None,
                user_defined_type_catalog=None,
                user_defined_type_schema=None,
                user_defined_type_name=None,
                is_insertable_into="YES",
                is_typed="YES",
                columns=[
                    Column(
                        table_catalog=DATABASE_NAME,
                        table_schema=SCHEMA,
                        table_name=TABLE_NAME,
                        column_name="NAME",
                        ordinal_position=1,
                        column_default=None,
                        is_nullable="YES",
                        data_type="TEXT",
                        character_maximum_length=16777216,
                        character_octet_length=16777216,
                        numeric_precision=None,
                        numeric_precision_radix=None,
                        numeric_scale=None,
                        collation_name=None,
                        is_identity="NO",
                        identity_generation=None,
                        identity_start=None,
                        identity_increment=None,
                        identity_cycle=None,
                        comment=None,
                    )
                ],
            ),
            View(
                upstream=[
                    Connection(
                        table_catalog=DATABASE_NAME,
                        table_name=TABLE_NAME,
                        table_schema=SCHEMA,
                        domain="TABLE",
                    )
                ],
                downstream=[
                    Connection(
                        table_catalog=DATABASE_NAME,
                        table_name=SECOND_VIEW,
                        table_schema=SCHEMA,
                        domain="VIEW",
                    )
                ],
                table_catalog=DATABASE_NAME,
                table_schema=SCHEMA,
                table_name=FIRST_VIEW,
                table_owner="ACCOUNTADMIN",
                table_type="VIEW",
                is_transient=None,
                clustering_key=None,
                row_count=None,
                retention_time=None,
                created=datetime.datetime.now(),
                last_altered=datetime.datetime.now(),
                table_comment=None,
                self_referencing_column_name=None,
                reference_generation=None,
                user_defined_type_catalog=None,
                user_defined_type_schema=None,
                user_defined_type_name=None,
                is_insertable_into="YES",
                is_typed="YES",
                columns=[
                    Column(
                        table_catalog=DATABASE_NAME,
                        table_schema=SCHEMA,
                        table_name=FIRST_VIEW,
                        column_name="NAME",
                        ordinal_position=1,
                        column_default=None,
                        is_nullable="YES",
                        data_type="TEXT",
                        character_maximum_length=16777216,
                        character_octet_length=16777216,
                        numeric_precision=None,
                        numeric_precision_radix=None,
                        numeric_scale=None,
                        collation_name=None,
                        is_identity="NO",
                        identity_generation=None,
                        identity_start=None,
                        identity_increment=None,
                        identity_cycle=None,
                        comment=None,
                    )
                ],
                view_definition="create view test_view as\n    -- comment = '<comment>'\n    select * from test;",
                is_updatable="NO",
                is_secure="NO",
                view_comment=None,
            ),
            View(
                upstream=[
                    Connection(
                        table_catalog=DATABASE_NAME,
                        table_name=FIRST_VIEW,
                        table_schema=SCHEMA,
                        domain="VIEW",
                    )
                ],
                downstream=[],
                table_catalog=DATABASE_NAME,
                table_schema=SCHEMA,
                table_name=SECOND_VIEW,
                table_owner="ACCOUNTADMIN",
                table_type="VIEW",
                is_transient=None,
                clustering_key=None,
                row_count=None,
                retention_time=None,
                created=datetime.datetime.now(),
                last_altered=datetime.datetime.now(),
                table_comment=None,
                self_referencing_column_name=None,
                reference_generation=None,
                user_defined_type_catalog=None,
                user_defined_type_schema=None,
                user_defined_type_name=None,
                is_insertable_into="YES",
                is_typed="YES",
                columns=[
                    Column(
                        table_catalog=DATABASE_NAME,
                        table_schema=SCHEMA,
                        table_name=SECOND_VIEW,
                        column_name="NAME",
                        ordinal_position=1,
                        column_default=None,
                        is_nullable="YES",
                        data_type="TEXT",
                        character_maximum_length=16777216,
                        character_octet_length=16777216,
                        numeric_precision=None,
                        numeric_precision_radix=None,
                        numeric_scale=None,
                        collation_name=None,
                        is_identity="NO",
                        identity_generation=None,
                        identity_start=None,
                        identity_increment=None,
                        identity_cycle=None,
                        comment=None,
                    )
                ],
                view_definition="create or replace view TEST_1.PUBLIC.TEST_VIEW_2(\n\tNAME\n) as\n    -- comment = '<comment>'\n    select * from TEST_VIEW;",
                is_updatable="NO",
                is_secure="NO",
                view_comment=None,
            ),
        ]
        return tables


@pytest.fixture()
def config() -> SnowflakePlugin:
    return SnowflakePlugin(
        name="snowflake_adapter",
        description="snowflake_adapter",
        host="localhost",
        database=DATABASE_NAME,
        user="omit",
        type="snowflake",
        password=SecretStr("omit"),
        account="account_name",
        warehouse="warehouse",
    )


@pytest.fixture()
def client(config: SnowflakePlugin) -> Type[SnowflakeClientBase]:
    return TestClient


def _find_database(seq: List[DataEntity]) -> Optional[DataEntity]:
    return first(filter(lambda entity: entity.name == DATABASE_NAME, seq))


def _find_schema(seq: List[DataEntity]) -> Optional[DataEntity]:
    return first(filter(lambda entity: entity.name == SCHEMA, seq))


def _find_tables(seq: List[DataEntity]) -> List[DataEntity]:
    return lfilter(
        lambda entity: entity.type in {DataEntityType.TABLE, DataEntityType.VIEW}, seq
    )


def test_adapter(config: SnowflakePlugin, client: Type[SnowflakeClientBase]):
    adapter = Adapter(config, client)

    data_entity_list = adapter.get_data_entity_list()
    data_entities = data_entity_list.items

    assert len(data_entities) == 5  # 3 Tables(Views) 1 Schema 1 Database

    database_entity: DataEntity = _find_database(data_entities)
    schema_entity: DataEntity = _find_schema(data_entities)
    tables_entities: List[DataEntity] = _find_tables(data_entities)

    assert database_entity is not None
    assert schema_entity.oddrn in database_entity.data_entity_group.entities_list

    assert schema_entity is not None
    for table_entity in tables_entities:
        assert table_entity.oddrn in schema_entity.data_entity_group.entities_list

    assert tables_entities is not None
    assert len(tables_entities) == 3
