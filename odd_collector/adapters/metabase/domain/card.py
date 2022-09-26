from datetime import datetime
from typing import Optional

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

    def get_oddrn(self, generator: Generator) -> str:
        generator.set_oddrn_paths(
            collections=self.collection_id or "root", cards=self.id
        )
        return generator.get_oddrn_by_path("cards")

    def get_owner(self) -> str:
        return self.creator.common_name
