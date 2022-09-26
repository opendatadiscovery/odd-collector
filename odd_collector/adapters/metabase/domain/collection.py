from typing import List, Optional, Union

from pydantic import BaseModel


class Collection(BaseModel):
    id: Union[int, str]
    name: str
    description: Optional[str]
    slug: Optional[str]
    archived: Optional[bool]
    cards_id: Optional[List[int]] = []
    dashboards_id: Optional[List[int]] = []
