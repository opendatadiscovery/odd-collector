from aiohttp import ClientSession
from odd_models.models import DataEntityList

from .http_client import HttpClient
from ..domain.register_datasource_request import RegisterDataSourceRequests


class DataSourceApi:
    def __init__(self, http_client: HttpClient, platform_url: str) -> None:
        self.__client = http_client
        self.__platform_url = platform_url

    async def register_datasource(
        self, requests: RegisterDataSourceRequests, session: ClientSession
    ):
        resp = await self.__client.post(
            f"{self.__platform_url}/ingestion/datasource",
            requests.json(),
            session,
        )

        resp.raise_for_status()

        return resp

    async def ingest_data(
        self, data_entity_list: DataEntityList, session: ClientSession
    ):
        resp = await self.__client.post(
            f"{self.__platform_url}/ingestion/entities",
            data_entity_list.json(),
            session,
        )

        return resp
