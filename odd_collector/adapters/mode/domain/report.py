from dataclasses import dataclass
from typing import Dict, Any, Optional
from ..generator import Generator


@dataclass
class Report:

    token: str
    id: int
    name: str
    created_at: str
    updated_at: str
    published_at: str
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
    _links: dict

    description: Optional[str] = None
    theme_id: Optional[int] = None
    color_mappings: Optional[dict] = None
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

    @staticmethod
    def from_response(response: Dict[str, Any]):
        report = Report(
            token=response.get("token"),
            id=response.get("id"),
            name=response.get("name"),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            published_at=response.get("published_at"),
            edited_at=response.get("edited_at"),
            type=response.get("type"),
            last_saved_at=response.get("last_saved_at"),
            archived=response.get("archived"),
            account_id=response.get("account_id"),
            account_username=response.get("account_username"),
            public=response.get("public"),
            manual_run_disabled=response.get("manual_run_disabled"),
            run_privately=response.get("run_privately"),
            drilldowns_enabled=response.get("drilldowns_enabled"),
            expected_runtime=response.get("expected_runtime"),
            last_successfully_run_at=response.get("last_successfully_run_at"),
            last_run_at=response.get("last_run_at"),
            last_successful_run_token=response.get("last_successful_run_token"),
            query_count=response.get("query_count"),
            max_query_count=response.get("max_query_count"),
            runs_count=response.get("runs_count"),
            query_preview=response.get("query_preview"),
            view_count=response.get("view_count"),
            _links=response.get("_links"),

            description=response.get("description"),
            theme_id=response.get("theme_id"),
            color_mappings=response.get("color_mappings"),
            last_successful_sync_at=response.get("last_successful_sync_at"),
            space_token=response.get("space_token"),
            full_width=response.get("full_width"),
            layout=response.get("layout"),
            is_embedded=response.get("is_embedded"),
            is_signed=response.get("is_signed"),
            shared=response.get("shared"),
            web_preview_image=response.get("web_preview_image"),
            flamingo_signature=response.get("flamingo_signature"),
            github_link=response.get("github_link"),
            chart_count=response.get("chart_count"),
            schedules_count=response.get("schedules_count"),
        )
        return report

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("reports", self.name)
        return oddrn_generator.get_oddrn_by_path("reports")
