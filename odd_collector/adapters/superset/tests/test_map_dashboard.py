import pytest
from oddrn_generator.generators import SupersetGenerator

from odd_collector.adapters.superset.client import SupersetClient
from odd_collector.adapters.superset.domain.chart import Chart
from odd_collector.adapters.superset.domain.dataset import Dataset
from odd_collector.adapters.superset.mappers.dashboard import map_dashboard

from .raw_data import chart_nodes, datasets_nodes, nodes_with_chart_ids


@pytest.fixture
def generator():
    return SupersetGenerator(host_settings="host")


def test_create_dashboards_entities(generator):
    charts = [
        Chart(
            id=chart_node["id"],
            dataset_id=chart_node["datasource_id"],
            dashboards_ids_names=nodes_with_chart_ids[chart_node["id"]],
        )
        for chart_node in chart_nodes
    ]
    datasets = {
        dataset[
            "id"
        ]: f"//superset/host/host/databases/{dataset['database']['database_name']}/datasets/{dataset['table_name']}"
        for dataset in datasets_nodes
    }
    dashboards = SupersetClient.extract_dashboards_from_charts(charts)

    dashboards_entities = [
        map_dashboard(generator, datasets, dashboard) for dashboard in dashboards
    ]
    assert len(dashboards_entities) == 2
    assert dashboards_entities[0].oddrn == "//superset/host/host/dashboards/dash_10"
    assert len(dashboards_entities[0].data_consumer.inputs) == 2
    assert len(dashboards_entities[1].data_consumer.inputs) == 1

    assert (
        dashboards_entities[0].data_consumer.inputs[0]
        == "//superset/host/host/databases/examples/datasets/threads"
    )
    assert (
        dashboards_entities[0].data_consumer.inputs[1]
        == "//superset/host/host/databases/jj/datasets/channels"
    )
    assert (
        dashboards_entities[1].data_consumer.inputs[0]
        == "//superset/host/host/databases/examples/datasets/pppp"
    )

    pass
