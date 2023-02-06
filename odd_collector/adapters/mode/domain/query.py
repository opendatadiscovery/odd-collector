from typing import Any, Dict, Optional

from pydantic import BaseModel


class Query(BaseModel):
    id: str
    token: str
    raw_query: str
    created_at: str
    updated_at: str
    name: str
    last_run_id: str
    data_source_id: str
    explorations_count: str
    links: dict

    mapping_id: Optional[str]

    @staticmethod
    def from_response(response: Dict[str, Any]):
        response["links"] = response.pop("_links")
        return Query.parse_obj(response)
