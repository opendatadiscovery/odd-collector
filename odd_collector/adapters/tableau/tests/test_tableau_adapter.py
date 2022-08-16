from typing import Dict, List

import pytest
from odd_models.models import DataEntity, DataEntityType
from pydantic import SecretStr

from odd_collector.adapters.tableau.adapter import Adapter, tables_ids_to_load
from odd_collector.adapters.tableau.client import TableauBaseClient
from odd_collector.adapters.tableau.domain.column import Column
from odd_collector.adapters.tableau.domain.sheet import Sheet
from odd_collector.adapters.tableau.domain.table import (
    BigqueryTable,
    EmbeddedTable,
    Table,
)
from odd_collector.domain.plugin import TableauPlugin


class TableauTestClient(TableauBaseClient):
    def get_tables_columns(self, tables_ids: List[str]) -> Dict[str, List[Column]]:
        return {
            "table-id-1": [
                Column(
                    id="1",
                    name="Age",
                    remote_type="I1",
                    is_nullable=False,
                    description="some description",
                ),
                Column(
                    id="1",
                    name="Height",
                    remote_type="I1",
                    is_nullable=True,
                    description="some description",
                ),
            ]
        }

    def __init__(self, config: TableauPlugin):
        self.config = config

    def get_tables(self) -> List[Table]:
        return [
            EmbeddedTable(
                id="table-id-1",
                name="table-name",
                schema="schema",
                db_id="db-id-1",
                db_name="db-name",
                connection_type="textscan",
                columns=[],
                owners=["user1", "user2"],
                description="table description",
            )
        ]

    def get_sheets(self) -> List[Sheet]:
        return [
            Sheet(
                id="id",
                name="name",
                workbook="workbook",
                owner="pmakarichev",
                created="2022-08-09T08:12:45Z",
                updated="2022-08-09T08:12:45Z",
                table_ids=["table-id-1"],
            )
        ]

    def get_server_host(self):
        return "host"


@pytest.fixture
def client():
    return TableauTestClient


@pytest.fixture
def config() -> TableauPlugin:
    return TableauPlugin(
        name="tableau",
        description="teableau_adapter",
        namespace="odd",
        type="tableau",
        server="server",
        site="site",
        user="user",
        password=SecretStr("password"),
    )


def test_adapter(config, client):
    adapter = Adapter(config, client)

    data_entity_list = adapter.get_data_entity_list()
    assert len(data_entity_list.items) == 2

    table_elements = [
        de for de in data_entity_list.items if de.type == DataEntityType.TABLE
    ]

    assert len(table_elements) == 1
    table: DataEntity = table_elements[0]
    assert len(table.dataset.field_list) == 2


def test_tables_ids_to_load():
    tables = [
        EmbeddedTable(
            id="table-id-1",
            name="table-name",
            schema="schema",
            db_id="db-id-1",
            db_name="db-name",
            connection_type="textscan",
            columns=[],
            owners=["user1", "user2"],
            description="table description",
        ),
        BigqueryTable(
            id="table-id-2",
            name="bigquery-table",
            schema="schema",
            db_id="db-id-2",
            db_name="db-name-2",
            connection_type="bigquery",
            columns=[],
            owners=["user1", "user2"],
            description="table description",
        ),
    ]

    ids = tables_ids_to_load(tables)

    assert len(ids) == 1
    assert ids[0] == "table-id-1"
