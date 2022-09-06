import pytest
from oddrn_generator.generators import SupersetGenerator
from odd_collector.adapters.superset.mappers.dashboards import (
    map_dashboard,
)
from odd_collector.adapters.superset.domain.dataset import Dataset
from odd_collector.adapters.superset.domain.chart import Chart
from odd_collector.adapters.superset.client import SupersetClient
from .raw_data import datasets_nodes, chart_nodes, nodes_with_chart_ids


@pytest.fixture
def generator():
    return SupersetGenerator(
        host_settings="host",
    )


def test_create_dashboards_entities(generator):
    charts = [
        Chart(
            id=chart_node.get("id"),
            dataset_id=chart_node.get("datasource_id"),
            dashboards_ids_names=nodes_with_chart_ids.get(chart_node.get("id")),
        )
        for chart_node in chart_nodes
    ]

    datasets = [
        Dataset(
            id=dataset.get("id"),
            name=dataset.get("table_name"),
            db_id=dataset.get("database").get("id"),
            db_name=dataset.get("database").get("database_name"),
            kind=dataset.get("kind"),
        )
        for dataset in datasets_nodes
    ]
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
