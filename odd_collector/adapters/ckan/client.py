import aiohttp
from odd_collector_sdk.errors import DataSourceError
from odd_collector.domain.plugin import CKANPlugin
from .logger import logger
import ssl

from .mappers.models import Organization, CKANDataset

# Disable SSL context verification for testing
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


class CKANRestClient:
    def __init__(self, config: CKANPlugin):
        self.__host = f"https://{config.host}:{config.port}"
        self.__headers = (
            {"Authorization": f"Bearer {config.token.get_secret_value()}"}
            if config.token
            else None
        )

    async def _get_request(self, url: str, params: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host,
            headers=self.__headers,
            connector=aiohttp.TCPConnector(ssl=ssl_context),
        ) as session:
            try:
                async with session.get(url, params=params) as resp:
                    result = await resp.json()
                    logger.debug(f"Result of request {url} is {result}")
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from workspace {self.__host}"
                ) from e

    async def get_organizations(self) -> list[str]:
        url = "/api/action/organization_list"
        resp = await self._get_request(url)
        if resp and resp["success"]:
            return resp["result"]
        return []

    async def get_organization_details(self, organization_name: str) -> Organization:
        url = "/api/action/organization_show"
        params = {"id": organization_name}
        resp = await self._get_request(url, params)
        if resp and resp["success"]:
            return Organization(resp["result"])
        return {}

    async def get_datasets(self, organization_id) -> list[CKANDataset]:
        url = "/api/3/action/package_search"
        params = {"q": f"owner_org:{organization_id}"}
        resp = await self._get_request(url, params)
        if resp and resp["success"]:
            return [CKANDataset(dataset) for dataset in resp["result"]["results"]]
        return []
