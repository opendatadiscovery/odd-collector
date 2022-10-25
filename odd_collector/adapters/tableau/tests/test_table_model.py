import pytest
from oddrn_generator import TableauGenerator

from odd_collector.adapters.tableau.domain.table import BigqueryTable, EmbeddedTable


@pytest.fixture
def generator():
    return TableauGenerator(host_settings="host", sites="site")


def test_emdedded_table_oddrn_generation(generator):
    table = EmbeddedTable(
        id="table-id-1",
        name="table-name",
        schema="schema",
        db_id="db-id-1",
        db_name="db-name",
        connection_type="textscan",
        columns=[],
        owners=["user1", "user2"],
        description="table description",
    )

    assert (
        table.get_oddrn(generator)
        == "//tableau/host/host/sites/site/databases/db-id-1/schemas/schema/tables/table-name"
    )


def test_biqquery_table_oddrn_generation(generator):
    table = BigqueryTable(
        id="table-id-1",
        name="table-name",
        schema="schema",
        db_id="db-id-1",
        db_name="db-name",
        connection_type="tableau",
        columns=[],
        owners=["user1", "user2"],
        description="table description",
    )

    assert (
        table.get_oddrn(generator)
        == "//bigquery_storage/cloud/gcp/project/db-name/datasets/schema/tables/table-name"
    )
