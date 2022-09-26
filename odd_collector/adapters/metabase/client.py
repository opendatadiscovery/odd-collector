import asyncio
from contextlib import asynccontextmanager
from operator import itemgetter
from typing import Any, Dict, List, Type, TypeVar

from aiohttp import ClientSession
from funcy import group_by, lmap, lpluck, lpluck_attr
from odd_collector_sdk.errors import DataSourceAuthorizationError

from odd_collector.domain.plugin import MetabasePlugin

from .domain import Card, Collection, Dashboard, Table
from .logger import logger

T = TypeVar("T", Card, Dashboard, Table, Collection)


class MetabaseClient:
    __SESSION_ENDPOINT = "/api/session"
    __DASHBOARD_ENDPOINT = "/api/dashboard"
    __CARD_ENDPOINT = "/api/card"
    __TABLE_ENDPOINT = "/api/table"
    __COLLECTION_ENDPOINT = "/api/collection"

    def __init__(
        self,
        config: MetabasePlugin,
    ) -> None:
        self.config = config

    @property
    def base_url(self):
        return f"{self.config.host}:{self.config.port}"

    async def get_dashboard_by(self, session: ClientSession, dashboard_id: int):
        """Get dashboards details from Metabase"""
        response = await session.get("/api/dashboard/3")
        return await response.json()

    async def get_dashboards(self, session: ClientSession) -> List[Dashboard]:
        dashboards = await self._get_resources(
            session, self.__DASHBOARD_ENDPOINT, Dashboard
        )
        # Need to take information which cards has dashboard
        ids = lpluck_attr("id", dashboards)
        tasks = [asyncio.create_task(self.get_dashboard_by(session, id)) for id in ids]
        dashboards_list = await asyncio.gather(*tasks)

        for idx, dashboard in enumerate(dashboards_list):
            ordered_cards = lpluck("card", dashboard.get("ordered_cards", []))

            cards = lmap(Card.parse_obj, ordered_cards)
            dashboards[idx].cards = cards

        return dashboards

    async def get_tables(self, session: ClientSession) -> List[Table]:
        """Get tables from Metabase"""
        return await self._get_resources(session, self.__TABLE_ENDPOINT, Table)

    async def get_collections(self, session: ClientSession) -> List[Collection]:
        """Get collections from Metabase"""
        collections = await self._get_resources(
            session, self.__COLLECTION_ENDPOINT, Collection
        )

        for collection in collections:
            response = await self._get_collection_items(session, collection.id)
            data = response.get("data")
            items = group_by(itemgetter("model"), data)

            collection.cards_id = lpluck("id", items.get("card", []))
            collection.dashboards_id = lpluck("id", items.get("dashboard", []))

        return collections

    async def get_cards(self, session: ClientSession) -> List[Card]:
        """Get cards from Metabase"""
        return await self._get_resources(session, self.__CARD_ENDPOINT, Card)

    async def _get_collection_items(
        self, session: ClientSession, collection_id: int
    ) -> Dict[str, Any]:
        response = await session.get(
            f"{self.__COLLECTION_ENDPOINT}/{collection_id}/items"
        )
        return await response.json()

    async def _get_resources(
        self, session: ClientSession, endpoint: str, model: Type[T]
    ) -> List[T]:
        logger.debug("Getting %s", endpoint.split("/")[-1])
        response = await session.get(endpoint)
        entities = await response.json()
        return lmap(model.parse_obj, entities)

    @asynccontextmanager
    async def session(self):
        """Context manager to manage aiohttp and metabase session at one place"""
        metabase_session_id = None
        logger.debug("Start aiohttp session")

        async with ClientSession(self.base_url) as session:
            try:
                logger.debug("Creating metabase session")
                resp = await session.post(
                    self.__SESSION_ENDPOINT,
                    json={
                        "username": self.config.login,
                        "password": self.config.password.get_secret_value(),
                    },
                )

                if not resp.ok:
                    raise DataSourceAuthorizationError("Could not connect to Metabase")

                # TODO: for auth
                metabase_session = await resp.json()
                metabase_session_id = metabase_session["id"]

                yield session
            finally:
                logger.debug("Delete Metabase session")
                await session.delete(
                    self.__SESSION_ENDPOINT,
                    params={"metabase-session-id": metabase_session_id},
                )
