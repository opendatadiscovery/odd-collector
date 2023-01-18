from abc import ABC
from typing import List

from aiohttp import ClientSession, BasicAuth
from odd_collector_sdk.errors import DataSourceAuthorizationError

from ...domain.plugin import ModePlugin
from .domain.report import Report


class ModeRepositoryBase(ABC):
    def __init__(self, config: ModePlugin):
        self.config = config
        self._host = config.host
        self._account = config.account
        self._data_source = config.data_source
        self._token = config.token
        self._password = config.password

    async def get_reports(self) -> List:
        raise NotImplementedError


class ModeRepository(ModeRepositoryBase):
    API_PATH = "%s/api/account"

    async def get_reports(self):
        headers = {}
        auth = BasicAuth(self._token.get_secret_value(), self._password.get_secret_value())

        async with ClientSession(headers=headers, auth=auth) as session:
            try:
                async with session.get(
                    f"{self._host}/api/{self._account}/data_sources/{self._data_source}/reports"
                ) as response:
                    result = await response.json()

                    reports = [Report.from_response(report) for report in result.get("_embedded").get("reports")]
                    return reports
            except Exception as e:
                # TODO: add more exceptions
                raise e
