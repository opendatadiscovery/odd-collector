import aiohttp
from odd_collector_sdk.errors import DataSourceError
from odd_collector.domain.plugin import CKANPlugin
from .logger import logger
from asyncio import gather

from .mappers.models import Organization, Dataset, Group, ResourceField


class CKANRestClient:
    def __init__(self, config: CKANPlugin):
        self.__host = f"https://{config.host}:{config.port}"
        self.__headers = (
            {"Authorization": config.token.get_secret_value()} if config.token else None
        )

    @staticmethod
    def is_response_successful(resp: dict):
        if resp and resp.get("success"):
            return True
        return False

    async def _get_request(self, url: str, params: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host,
            headers=self.__headers,
        ) as session:
            try:
                async with session.get(url, params=params) as resp:
                    result = await resp.json()
                    logger.debug(f"Result of request {url} is {result}")
                    if not self.is_response_successful(result):
                        raise DataSourceError(
                            f"Request: {url}, Error: {result['error']}"
                        )
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from host {self.__host}: {e}"
                ) from e

    async def _post_request(self, url: str, payload: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host,
            headers=self.__headers,
        ) as session:
            try:
                async with session.post(url, json=payload) as resp:
                    result = await resp.json()
                    logger.debug(f"Result of request {url} is {result}")
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from host {self.__host}: {e}"
                ) from e

    async def get_organizations(self) -> list[Organization]:
        url = "/api/action/organization_list"
        resp = await self._get_request(url)
        org_names = resp["result"]
        response = await gather(
            *[
                self.get_organization_details(organization_name)
                for organization_name in org_names
            ]
        )
        return response

    async def get_organization_details(self, organization_name: str) -> Organization:
        url = "/api/action/organization_show"
        params = {"id": organization_name}
        resp = await self._get_request(url, params)
        return Organization(resp["result"])

    async def get_groups(self) -> list[str]:
        url = "/api/action/group_list"
        resp = await self._get_request(url)
        return resp["result"]

    async def get_group_details(self, group_name: str) -> Group:
        url = "/api/action/group_show"
        params = {"id": group_name, "include_datasets": "True"}
        resp = await self._get_request(url, params)
        return Group(resp["result"])

    async def get_datasets(self, organization_id: str) -> list[Dataset]:
        url = "/api/action/package_search"
        params = {"q": f"owner_org:{organization_id}", "include_private": "True"}
        resp = await self._get_request(url, params)
        return [Dataset(dataset) for dataset in resp["result"]["results"]]

    async def get_resource_fields(self, resource_id: str) -> list[ResourceField]:
        url = "/api/action/datastore_info"
        payload = {"id": resource_id}
        resp = await self._post_request(url, payload)
        if self.is_response_successful(resp):
            return [ResourceField(field) for field in resp["result"]["fields"]]
        return []
