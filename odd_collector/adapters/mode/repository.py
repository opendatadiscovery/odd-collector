import posixpath

from abc import ABC
from typing import List, Dict

from aiohttp import ClientSession, BasicAuth

from .domain.query import Query
from ...domain.plugin import ModePlugin
from .domain.report import Report
from .domain.datasource import DataSource
from .domain.collection import Collection
from ...domain.rest_client.client import RestClient, RequestArgs


class ModeRepositoryBase(ABC):
    def __init__(self, config: ModePlugin):
        self.config = config
        self._host = config.host
        self._account = config.account
        self._data_source = config.data_source
        self._token = config.token
        self._password = config.password

    async def _get_requests(self, path) -> Dict:
        raise NotImplementedError

    async def get_data_sources(self) -> List[DataSource]:
        raise NotImplementedError

    async def get_collections(self) -> List[Collection]:
        raise NotImplementedError

    async def get_reports_for_data_source(self, data_source) -> List[Report]:
        raise NotImplementedError

    async def get_reports_for_space(self, data_source) -> List[Report]:
        raise NotImplementedError

    async def get_queries_for_reports(self, report) -> List[Query]:
        raise NotImplementedError


class ModeRepository(ModeRepositoryBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_path = f"{self._host}/api/{self._account}"
        self.rest_client = RestClient()

    async def _get_requests(self, path: str) -> Dict:
        path = path[1:] if path.startswith("/") else path
        auth = BasicAuth(self._token.get_secret_value(), self._password.get_secret_value())
        request_args = RequestArgs(
            method="GET",
            url=posixpath.join(self.api_path, path),
            headers={f"Authorization": auth.encode()}
        )
        async with ClientSession() as session:
            result = await self.rest_client.fetch_async_response(session, request_args)
        return result

    async def get_data_sources(self) -> List[DataSource]:
        path = "data_sources"
        result = await self._get_requests(path)
        result = result.get("_embedded").get("data_sources")
        data_sources = [DataSource.from_response(data_source) for data_source in result]
        return data_sources

    async def get_collections(self) -> List[Collection]:
        path = "spaces"
        result = await self._get_requests(path)
        result = result.get("_embedded").get("spaces")
        collections = [Collection.from_response(collection) for collection in result]
        return collections

    async def get_reports_for_data_source(self, data_source: DataSource) -> List[Report]:
        path = f"data_sources/{data_source.token}/reports"
        result = await self._get_requests(path)
        reports_tokens = set()
        reports = []
        for report_json in result.get("_embedded").get("reports"):
            report = Report.from_response(report_json)
            report = report.set_db_setting(data_source)

            # there is a bug from MODE side: MODE returns duplicates
            # of reports in API response for unknown reason.
            if report.token not in reports_tokens:
                queries = await self.get_queries_for_reports(report.token)
                report.queries = queries
                reports.append(report)
                reports_tokens.add(report.token)

        return reports

    async def get_reports_for_space(self, collection: Collection) -> List[Collection]:
        # this method doesn't set up db setting for reports as
        path = f"spaces/{collection.token}/reports"
        result = await self._get_requests(path)
        reports = []
        for report_json in result.get("_embedded").get("reports"):
            report = Report.from_response(report_json)
            reports.append(report)
        return reports

    async def get_queries_for_reports(self, report: str) -> List[Query]:
        path = f"reports/{report}/queries"
        result = await self._get_requests(path)
        result = result.get("_embedded").get("queries")
        queries = [Query.from_response(query) for query in result]
        return queries
