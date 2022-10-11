import pytest
from odd_models.models import DataEntityType
from oddrn_generator.generators import PrestoGenerator

from odd_collector.adapters.presto.mappers.catalogs import map_catalog

from .raw_data import nested_nodes


@pytest.fixture
def generator():
    return PrestoGenerator(host_settings="localhost")


def test_map_catalog(generator):
    catalog_node_name = "mysql"
    data_entity = map_catalog(
        generator, catalog_node_name, nested_nodes[catalog_node_name]
    )
    assert data_entity.oddrn == "//presto/host/localhost/catalogs/mysql"
    assert data_entity.type == DataEntityType.DATABASE_SERVICE
    entities_list = data_entity.data_entity_group.entities_list
    assert len(entities_list) == 2
    assert (
        entities_list[1]
        == "//presto/host/localhost/catalogs/mysql/schemas/test_schema_mysql2"
    )
