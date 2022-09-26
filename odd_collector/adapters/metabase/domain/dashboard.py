from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from odd_collector.adapters.metabase.domain import Card, Creator

keys_to_exclude = {"id", "creator", "last-edit-info"}


class Dashboard(BaseModel):
    id: int
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime] = None
    cards: Optional[List[Card]] = []
    creator: Creator
    collection_id: Optional[int]

    def get_owner(self) -> str:
        return self.creator.common_name
