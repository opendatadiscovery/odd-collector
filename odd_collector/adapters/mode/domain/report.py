from pydantic import BaseModel, validator
from sql_metadata import Parser

from typing import Dict, Any, Optional, List, Union

from .datasource import DataSource
from .query import Query
from ..generator import Generator


class TableSource:
    def __init__(self, table_path):
        split_table_name = table_path.split(".")
        self.table = split_table_name[-1]
        self.schema = split_table_name[-2]
        self.database = split_table_name[-3] if len(split_table_name) >= 3 else None
        self.server = split_table_name[-4] if len(split_table_name) >= 4 else None


class Report(BaseModel):
    token: str
    id: int
    created_at: str
    updated_at: str
    edited_at: str
    type: str
    last_saved_at: str
    archived: bool
    account_id: int
    account_username: str
    public: bool
    manual_run_disabled: bool
    run_privately: bool
    drilldowns_enabled: bool
    expected_runtime: float
    last_successfully_run_at: str
    last_run_at: str
    last_successful_run_token: str
    query_count: int
    max_query_count: int
    runs_count: int
    query_preview: str
    view_count: int
    links: dict

    name: Optional[str] = ""
    published_at: Optional[str]
    description: Optional[str] = None
    theme_id: Optional[int] = None
    color_mappings: Optional[Union[str, dict]] = None
    last_successful_sync_at: Optional[str] = None
    space_token: Optional[str] = None
    full_width: Optional[bool] = None
    layout: Optional[str] = None
    is_embedded: Optional[bool] = None
    is_signed: Optional[bool] = None
    shared: Optional[bool] = None
    web_preview_image: Optional[str] = None
    flamingo_signature: Optional[str] = None
    github_link: Optional[str] = None
    chart_count: Optional[int] = None
    schedules_count: Optional[int] = None
    queries: Optional[List[Query]] = None

    host: Optional[str] = None
    database: Optional[str] = None
    adapter: Optional[str] = None

    @staticmethod
    def from_response(response: Dict[str, Any]):
        response["links"] = response.pop("_links")
        return Report.parse_obj(response)

    def set_db_setting(self, data_source: DataSource):
        self.host = data_source.host
        self.database = data_source.database
        self.adapter = data_source.adapter
        return self

    @validator("name")
    def set_name(cls, name):
        return name or "Untitled Report"

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("reports", self.id)
        return oddrn_generator.get_oddrn_by_path("reports")

    def get_report_db_sources(self) -> List[TableSource]:
        tables = set()
        queries_str = [query.raw_query for query in self.queries]
        for query_str in queries_str:
            parser = Parser(query_str)
            tables.update(parser.tables)

        report_sources = [TableSource(table) for table in tables]
        return report_sources
