import pytest
from odd_models.models import DataEntityType
from oddrn_generator import VerticaGenerator

from odd_collector.adapters.vertica.domain.column import Column
from odd_collector.adapters.vertica.domain.table import Table
from odd_collector.adapters.vertica.mapper.columns import map_column
from odd_collector.adapters.vertica.mapper.tables import map_table
from odd_collector.adapters.vertica.mapper.types import TYPES_SQL_TO_ODD
from odd_collector.adapters.vertica.tests.test_vertica_adapter import (
    VerticaTestRepository,
)


@pytest.fixture()
def generator():
    return VerticaGenerator(host_settings="localhost", databases="VMart")


def test_mapping_table(generator):
    table_response = VerticaTestRepository._table_response[1]
    table = Table(*table_response)
    columns = [
        Column(*r)
        for r in VerticaTestRepository._column_response
        if r[1] == table.table_name
    ]
    table.columns = columns
    data_entity = map_table(generator, table)

    expected_table_path = f"//vertica/host/localhost/databases/VMart/schemas/{table.table_schema}/tables/{table.table_name}"
    assert data_entity.oddrn == expected_table_path
    assert data_entity.name == table_response[1]
    assert data_entity.owner == table_response[5]
    assert data_entity.description == table_response[3]
    assert data_entity.type == DataEntityType.TABLE
    assert data_entity.dataset is not None
    assert data_entity.dataset.parent_oddrn == expected_table_path
    assert len(data_entity.dataset.field_list) == len(columns)

    clm1 = data_entity.dataset.field_list[0]
    assert clm1.oddrn == f"{expected_table_path}/columns/{columns[0].column_name}"
    assert clm1.name == columns[0].column_name


def test_mapping_column(generator):
    table_response = VerticaTestRepository._table_response[2]
    table = Table(*table_response)
    column_response = VerticaTestRepository._column_response[0]
    column = Column(*column_response)
    map_table(generator, table)
    data_entity = map_column(generator, column, table.owner_name)

    assert (
        data_entity.oddrn
        == f"//vertica/host/localhost/databases/VMart/schemas/{column.table_schema}/tables/{column.table_name}/columns/{column.column_name}"
    )
    assert data_entity.name == column_response[4]
    assert data_entity.owner == table.owner_name
    assert data_entity.is_primary_key == column_response[6]
    assert data_entity.type.type == TYPES_SQL_TO_ODD[column.data_type]
