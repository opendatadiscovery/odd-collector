from datetime import datetime

from odd_models.models import DataEntityType

from odd_collector.adapters.metabase.domain import Collection
from odd_collector.adapters.metabase.generator import MetabaseGenerator
from odd_collector.adapters.metabase.mappers.collection import map_collection


def test_map_card():
    generator = MetabaseGenerator(host_settings="localhost")

    card = Collection()
    data_entity = map_card(card=card, table=None, generator=generator)

    assert data_entity.oddrn == "//metabase/host/localhost/collections/1/cards/1"
    assert data_entity.name == "test_card"
    assert data_entity.type == DataEntityType.FILE
