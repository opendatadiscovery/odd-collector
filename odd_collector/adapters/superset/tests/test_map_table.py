import pytest
from odd_models.models import DataEntityType
from oddrn_generator.generators import SupersetGenerator

from odd_collector.adapters.superset.domain.column import Column
from odd_collector.adapters.superset.domain.dataset import Dataset
from odd_collector.adapters.superset.mappers.datasets import map_table


@pytest.fixture
def generator():
    return SupersetGenerator(host_settings="host")


def test_table_to_data_entity(generator):
    table = Dataset(
        id=1,
        name="table-name",
        db_id=1,
        db_name="db-name",
        kind="virtual",
        columns=[
            Column(id=1, name="Age", remote_type="TEXT"),
            Column(id=3, name="Height", remote_type="INTEGER"),
        ],
        schema="schema",
        metadata=[],
    )

    data_entity = map_table(generator, table)

    assert (
        data_entity.oddrn
        == "//superset/host/host/databases/db-name/datasets/table-name"
    )
    assert data_entity.name == "table-name"
    assert data_entity.type == DataEntityType.VIEW

    assert data_entity.dataset is not None

    dataset = data_entity.dataset
    assert (
        dataset.parent_oddrn
        == "//superset/host/host/databases/db-name/datasets/table-name"
    )
    assert len(data_entity.dataset.field_list) == 2

    age_field = data_entity.dataset.field_list[0]
    assert age_field.name == "Age"
    assert (
        age_field.oddrn
        == "//superset/host/host/databases/db-name/datasets/table-name/columns/Age"
    )
