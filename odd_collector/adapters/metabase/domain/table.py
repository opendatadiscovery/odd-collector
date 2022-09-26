from typing import Optional

from pydantic import BaseModel, Field

from odd_collector.adapters.metabase.domain import Database


class Table(BaseModel):
    id: int
    name: str
    schem: str = Field(..., alias="schema")
    db: Database
    entity_type: Optional[str]
    description: Optional[str]
    display_name: str
