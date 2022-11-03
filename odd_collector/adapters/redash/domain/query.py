from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from .metadata import create_metadata_extension_list
from .. import (
    _METADATA_SCHEMA_URL_PREFIX,
    _keys_to_include_query,
)
from odd_models.models import MetadataExtension


@dataclass
class Query:
    id: int
    data_source_id: str
    name: str
    metadata: List[MetadataExtension]
    columns: Optional[List[str]] = None

    @staticmethod
    def from_response(node: Dict[str, Any]):
        return Query(
            id=node["id"],
            data_source_id=node["data_source_id"],
            name=node["name"],
            metadata=create_metadata_extension_list(
                _METADATA_SCHEMA_URL_PREFIX, node, _keys_to_include_query
            ),
        )

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(queries=self.name)
        return oddrn_generator.get_oddrn_by_path("queries")
