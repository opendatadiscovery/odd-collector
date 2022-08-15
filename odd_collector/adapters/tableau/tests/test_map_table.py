import pytest
from odd_models.models import DataEntityType
from oddrn_generator import TableauGenerator

from odd_collector.adapters.tableau.domain.column import Column
from odd_collector.adapters.tableau.domain.table import Table
from odd_collector.adapters.tableau.mappers.tables import map_table


@pytest.fixture
def generator():
    return TableauGenerator(host_settings="host", sites="site")


def test_table_to_data_entity(generator):
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
                id="2",
                name="Height",
                remote_type="I1",
                is_nullable=True,
                description="some description",
            ),
        ],
        owners=["user1", "user2"],
        description="table description",
    )

    data_entity = map_table(generator, table)

    assert (
        data_entity.oddrn
        == "//tableau/host/host/sites/site/databases/db-id-1/schemas/schema/tables/table-name"
    )
    assert data_entity.name == "table-name"
    assert data_entity.owner == "user1"
    assert data_entity.description == "table description"
    assert data_entity.type == DataEntityType.TABLE

    assert data_entity.dataset is not None

    dataset = data_entity.dataset
    assert (
        dataset.parent_oddrn
        == "//tableau/host/host/sites/site/databases/db-id-1/schemas/schema/tables/table-name"
    )
    assert len(data_entity.dataset.field_list) == 2

    age_field = data_entity.dataset.field_list[0]
    assert age_field.name == "Age"
    assert (
        age_field.oddrn
        == "//tableau/host/host/sites/site/databases/db-id-1/schemas/schema/tables/table-name/columns/Age"
    )
