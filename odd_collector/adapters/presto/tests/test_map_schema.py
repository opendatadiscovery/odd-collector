import pytest
from oddrn_generator.generators import PrestoGenerator
from odd_collector.adapters.presto.mappers.schemas import map_schema
from odd_models.models import DataEntityType
from .raw_data import nested_nodes

catalog_node_name = "mysql"


@pytest.fixture
def generator():
    return PrestoGenerator(host_settings="localhost", catalogs=catalog_node_name)


def test_map_catalog(generator):
    schema_node_name = "test_schema_mysql2"
    data_entity = map_schema(
        generator, schema_node_name, nested_nodes[catalog_node_name][schema_node_name]
    )
    assert (
        data_entity.oddrn
        == "//presto/host/localhost/catalogs/mysql/schemas/test_schema_mysql2"
    )
    assert data_entity.type == DataEntityType.DATABASE_SERVICE
    entities_list = data_entity.data_entity_group.entities_list
    assert len(entities_list) == 1
    assert (
        entities_list[0]
        == "//presto/host/localhost/catalogs/mysql/schemas/test_schema_mysql2/tables/test_table_mysql"
    )
