from datetime import datetime

from odd_models.models import DataEntityType

from odd_collector.adapters.metabase.domain import Card
from odd_collector.adapters.metabase.generator import MetabaseGenerator
from odd_collector.adapters.metabase.mappers.card import map_card


def test_map_card():
    generator = MetabaseGenerator(host_settings="localhost")

    card = Card(
        id=1,
        table_id=1,
        description=None,
        name="test_card",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        creator=None,
        query_type="table",
        display="display",
        entity_id="some_id",
        archived=False,
        collection_id=1,
        collection_position=1,
        result_metadata=None,
        can_write=True,
        enable_embedding=True,
        dashboard_count=0,
        average_query_time=0,
        collection_preview=None,
    )
    data_entity = map_card(card=card, table=None, generator=generator)

    assert data_entity.oddrn == "//metabase/host/localhost/collections/1/cards/1"
    assert data_entity.name == "test_card"
    assert data_entity.type == DataEntityType.FILE
