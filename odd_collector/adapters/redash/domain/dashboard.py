from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime
from dateutil import parser


@dataclass
class Dashboard:
    tags: List[str]
    id: int
    name: str
    created_at: datetime
    updated_at: datetime
    slug: str
    queries_ids: List[int]

    @staticmethod
    def from_response(node: Dict[str, Any]):
        _dt_format = "%Y-%m-%dT%H:%M:%S"
        return Dashboard(
            tags=node['tags'],
            id=node['id'],
            name=node['name'],
            created_at=parser.isoparse(node['created_at']),
            updated_at=parser.isoparse(node['updated_at']),
            slug=node['slug'],
            queries_ids=[widget['visualization']['query']['id'] for widget in node['widgets']]
        )
