import pytest
from oddrn_generator import VerticaGenerator

from odd_collector.adapters.vertica.domain.table import Table
from odd_collector.adapters.vertica.tests.test_vertica_adapter import VerticaTestRepository


@pytest.fixture()
def generator():
    return VerticaGenerator(host_settings='localhost', databases='VMart')


def test_parse_table(generator):
    table_response = VerticaTestRepository._table_response[0]
    table = Table(*table_response)
    assert table.table_schema == table_response[0]
    assert table.table_name == table_response[1]
    assert table.table_type == table_response[2]
    assert table.description == table_response[3]
    assert table.table_id == table_response[4]
    assert table.owner_name == table_response[5]
    assert table.create_time == table_response[6]
    assert table.table_rows == table_response[7]
    assert table.is_temp_table == table_response[8]
    assert table.is_system_table == table_response[9]
    assert table.view_definition == table_response[10]
    assert table.is_system_view == table_response[11]
    assert table.get_oddrn(generator) == "//vertica/host/localhost/databases/VMart/schemas/public/tables/product_dimension"


def test_parse_view_oddrn_generation(generator):
    table_response = VerticaTestRepository._table_response[3]
    table = Table(*table_response)
    assert table.table_schema == table_response[0]
    assert table.table_name == table_response[1]
    assert table.table_type == table_response[2]
    assert table.description == table_response[3]
    assert table.table_id == table_response[4]
    assert table.owner_name == table_response[5]
    assert table.create_time == table_response[6]
    assert table.table_rows == table_response[7]
    assert table.is_temp_table == table_response[8]
    assert table.is_system_table == table_response[9]
    assert table.view_definition == table_response[10]
    assert table.is_system_view == table_response[11]
    assert table.get_oddrn(generator) == "//vertica/host/localhost/databases/VMart/schemas/public/views/test_view"

