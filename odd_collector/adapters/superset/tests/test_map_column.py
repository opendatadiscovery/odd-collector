import pytest
from odd_models.models import Type
from odd_collector.adapters.superset.plugin.plugin import SupersetGenerator

from odd_collector.adapters.superset.domain.column import Column
from odd_collector.adapters.superset.mappers.columns import map_column


@pytest.fixture
def generator():
    return SupersetGenerator(
        host_settings="host",
        databases="db",
        datasets="dataset",
    )


def test_map_column(generator):
    column = Column(
        id=1,
        name="Age",
        remote_type="TEXT",
    )
    data_entity = map_column(generator, column)

    assert (
        data_entity.oddrn
        == "//superset/host/host/databases/db/datasets/dataset/columns/Age"
    )

    assert data_entity.name == "Age"
    data_entity_type = data_entity.type

    assert data_entity_type.type == Type.TYPE_UNKNOWN
    assert data_entity_type.logical_type == "TEXT"
    assert data_entity_type.is_nullable == False
