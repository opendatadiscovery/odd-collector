from dataclasses import dataclass
from typing import Any, Dict, List

from odd_models.models import MetadataExtension

from .. import _METADATA_SCHEMA_URL_PREFIX, _keys_to_include_dashboard
from .metadata import create_metadata_extension_list


@dataclass
class Dashboard:
    id: int
    name: str
    slug: str
    metadata: List[MetadataExtension]
    queries_ids: List[int]

    @staticmethod
    def from_response(node: Dict[str, Any]):
        return Dashboard(
            id=node["id"],
            name=node["name"],
            slug=node["slug"],
            queries_ids=[
                widget["visualization"]["query"]["id"] for widget in node["widgets"]
            ],
            metadata=create_metadata_extension_list(
                _METADATA_SCHEMA_URL_PREFIX, node, _keys_to_include_dashboard
            ),
        )
