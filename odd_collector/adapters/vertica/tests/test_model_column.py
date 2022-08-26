import pytest

from odd_collector.adapters.vertica.domain.column import Column
from odd_collector.adapters.vertica.tests.test_vertica_adapter import VerticaTestRepository


def test_parse_column():
    column_response = VerticaTestRepository._column_response[0]
    column = Column(*column_response)
    assert len(column.__dict__) == len(column_response)
    assert column.table_schema == column_response[0]
    assert column.table_name == column_response[1]
    assert column.is_system_table == column_response[2]
    assert column.column_id == column_response[3]
    assert column.column_name == column_response[4]
    assert column.data_type == column_response[5]
    assert column.is_primary_key == column_response[6]
    assert column.data_type_id == column_response[7]
    assert column.data_type_length == column_response[8]
    assert column.character_maximum_length == column_response[9]
    assert column.numeric_precision == column_response[10]
    assert column.numeric_scale == column_response[11]
    assert column.datetime_precision == column_response[12]
    assert column.interval_precision == column_response[13]
    assert column.ordinal_position == column_response[14]
    assert column.is_nullable == column_response[15]
    assert column.column_default == column_response[16]
    assert column.column_set_using == column_response[17]
    assert column.is_identity == column_response[18]
    assert column.is_primary_key_enabled == column_response[19]
    assert column.description == column_response[20]