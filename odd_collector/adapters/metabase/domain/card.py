from datetime import datetime
from typing import Any, Dict, List, Optional

from oddrn_generator import Generator
from pydantic import BaseModel

from .creator import Creator


class Card(BaseModel):
    id: int
    table_id: Optional[int]
    description: Optional[str]
    name: str
    created_at: datetime
    updated_at: datetime
    creator: Optional[Creator]
    query_type: str
    display: str
    entity_id: str
    archived: bool
    collection_id: Optional[int]
    collection_position: Optional[int]
    result_metadata: Optional[List]
    can_write: Optional[bool]
    enable_embedding: bool
    query_type: Optional[str]
    dashboard_count: Optional[int]
    average_query_time: Optional[float]
    display: Optional[str]
    collection_preview: Optional[bool]

    def get_oddrn(self, generator: Generator) -> str:
        generator.set_oddrn_paths(
            collections=self.collection_id or "root", cards=self.id
        )
        return generator.get_oddrn_by_path("cards")

    def get_owner(self) -> Optional[str]:
        return self.creator.common_name if self.creator else None

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "description": self.description,
            "archived": self.archived,
            "collection_position": self.collection_position,
            "can_write": self.can_write,
            "enable_embedding": self.enable_embedding,
            "query_type": self.query_type,
            "dashboard_count": self.dashboard_count,
            "average_query_time": self.average_query_time,
            "display": self.display,
            "collection_preview": self.collection_preview,
        }
