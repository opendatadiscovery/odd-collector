from typing import List
from urllib import parse

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import MetabaseGenerator

from odd_collector.domain.plugin import MetabasePlugin

from .client import MetabaseClient
from .mappers.card import map_card
from .mappers.collection import map_collection
from .mappers.dashboard import map_dashboard


class Adapter(AbstractAdapter):
    def __init__(self, config: MetabasePlugin) -> None:
        self.config = config
        self.client = MetabaseClient(config)

        self.generator = MetabaseGenerator(
            host_settings=parse.urlparse(config.host).netloc
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        async with self.client.session() as session:
            dashboards = await self.client.get_dashboards(session)
            tables = await self.client.get_tables(session)
            cards = await self.client.get_cards(session)
            collections = await self.client.get_collections(session)

            entities: List[DataEntity] = []
            cards_entity_oddrn = {}
            for card in cards:
                cards_table = tables[card.table_id] if card.table_id else None
                card_entity = map_card(
                    card,
                    cards_table,
                    self.generator,
                )
                cards_entity_oddrn[card.id] = card_entity.oddrn
                entities.append(card_entity)

            for dashboard in dashboards:
                cards_oddrn = [cards_entity_oddrn[card.id] for card in dashboard.cards]
                dashboard_entity = map_dashboard(dashboard, self.generator, cards_oddrn)
                entities.append(dashboard_entity)

            for collection in collections:
                collection_entity = map_collection(collection, self.generator)
                entities.append(collection_entity)

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(), items=entities
            )
