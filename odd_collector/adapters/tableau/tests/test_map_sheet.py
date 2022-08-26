import datetime
from typing import List

import pytest
from funcy import first
from odd_models.models import DataEntity, DataEntityType
from oddrn_generator import TableauGenerator

from odd_collector.adapters.tableau.mappers.tables import map_table
from odd_collector.adapters.tableau.domain.column import Column
from odd_collector.adapters.tableau.domain.sheet import Sheet
from odd_collector.adapters.tableau.domain.table import Table
from odd_collector.adapters.tableau.mappers.sheets import map_sheet


@pytest.fixture
def generator():
    return TableauGenerator(host_settings="host", sites="site")


@pytest.fixture
def tables() -> List[DataEntity]:
    table = Table(
        id="table-id-1",
        name="table-name",
        schema="schema",
        db_id="db-id-1",
        db_name="db-name",
        connection_type="textscan",
        columns=[
            Column(
                id="1",
                name="Age",
                remote_type="I1",
                is_nullable=False,
                description="some description",
            ),
            Column(
                id="1",
                name="Age",
                remote_type="I1",
                is_nullable=False,
                description="some description",
            ),
        ],
        owners=["user1", "user2"],
        description="table description",
    )

    generator = TableauGenerator(host_settings="host", sites="site")

    return [map_table(generator, table)]


def test_map_sheet(generator, tables: List[DataEntity]):
    sheet = Sheet(
        id="id",
        name="name",
        workbook="workbook",
        owner="pmakarichev",
        created="2022-08-09T08:12:45Z",
        updated="2022-08-09T08:12:45Z",
        table_ids=["table-id-1"],
    )

    data_entity = map_sheet(generator, sheet, tables)
    assert (
        data_entity.oddrn
        == "//tableau/host/host/sites/site/workbooks/workbook/sheets/name"
    )
    assert data_entity.name == "name"
    assert data_entity.owner == "pmakarichev"
    assert data_entity.created_at == datetime.datetime(
        2022, 8, 9, 8, 12, 45, tzinfo=datetime.timezone.utc
    )
    assert data_entity.updated_at == datetime.datetime(
        2022, 8, 9, 8, 12, 45, tzinfo=datetime.timezone.utc
    )
    assert data_entity.type == DataEntityType.DASHBOARD
    assert data_entity.data_consumer is not None

    data_consumer = data_entity.data_consumer
    assert len(data_consumer.inputs) == 1
    assert (
        first(data_consumer.inputs)
        == "//tableau/host/host/sites/site/databases/db-id-1/schemas/schema/tables/table-name"
    )
