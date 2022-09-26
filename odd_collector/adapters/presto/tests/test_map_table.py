import pytest
from oddrn_generator.generators import PrestoGenerator
from odd_collector.adapters.presto.mappers.tables import map_table
from odd_collector.adapters.presto.mappers.models import TableMetadata
from pandas import DataFrame
from odd_models.models import DataEntityType
from odd_models.models import Type
from .raw_data import nested_nodes
from .raw_data import tables_nodes

catalog_node_name = "mysql"
schema_node_name = "test_schema_mysql2"


@pytest.fixture
def generator():
    return PrestoGenerator(
        host_settings="localhost", catalogs=catalog_node_name, schemas=schema_node_name
    )


def test_map_table(generator):
    table_node_name = "test_table_mysql"
    df_tables = DataFrame(tables_nodes)
    df_tables.columns = TableMetadata._fields
    data_entity = map_table(
        generator,
        table_node_name,
        nested_nodes[catalog_node_name][schema_node_name][table_node_name],
        df_tables,
        catalog_node_name,
        schema_node_name,
    )
    assert (
        data_entity.oddrn
        == "//presto/host/localhost/catalogs/mysql/schemas/test_schema_mysql2/tables/test_table_mysql"
    )
    assert data_entity.type == DataEntityType.TABLE
    entities_list = data_entity.dataset.field_list
    assert len(entities_list) == 2
    assert (
        entities_list[1].oddrn
        == "//presto/host/localhost/catalogs/mysql/schemas/test_schema_mysql2/tables/test_table_mysql/columns/value"
    )
    assert entities_list[1].type.type == Type.TYPE_STRING
    assert entities_list[1].type.logical_type == "varchar(10)"
