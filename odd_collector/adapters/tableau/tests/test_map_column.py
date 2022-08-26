import pytest
from odd_models.models import Type
from oddrn_generator import TableauGenerator

from odd_collector.adapters.tableau.domain.column import Column
from odd_collector.adapters.tableau.mappers.columns import map_column


@pytest.fixture
def generator():
    return TableauGenerator(
        host_settings="host",
        sites="site",
        databases="db",
        schemas="schema",
        tables="table",
    )


def test_map_column(generator):
    column = Column(
        id="1",
        name="Age",
        remote_type="I1",
        is_nullable=False,
        description="some description",
    )
    data_entity = map_column(generator, column)

    assert (
        data_entity.oddrn
        == "//tableau/host/host/sites/site/databases/db/schemas/schema/tables/table/columns/Age"
    )

    assert data_entity.name == "Age"
    assert data_entity.description == "some description"
    data_entity_type = data_entity.type

    assert data_entity_type.type == Type.TYPE_INTEGER
    assert data_entity_type.logical_type == "I1"
    assert data_entity_type.is_nullable == False
