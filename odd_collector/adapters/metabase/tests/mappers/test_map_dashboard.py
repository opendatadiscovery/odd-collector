from datetime import datetime

from odd_collector.adapters.metabase.domain import Creator, Dashboard
from odd_collector.adapters.metabase.generator import MetabaseGenerator
from odd_collector.adapters.metabase.mappers.dashboard import map_dashboard


def test_map_dashboard():
    generator = MetabaseGenerator(host_settings="localhost")
    dashboard = Dashboard(
        id=1,
        name="test_dashboard",
        description="description",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        cards=[],
        creator=Creator(email="abs@mail.com", id=1, common_name="Some Name"),
        collection_id=1,
    )

    data_entity = map_dashboard(dashboard, generator, [])

    assert data_entity.oddrn == "//metabase/host/localhost/collections/1/dashboards/1"
