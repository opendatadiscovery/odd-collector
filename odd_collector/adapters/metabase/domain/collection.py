from typing import List, Optional, Union, Dict, Any

from pydantic import BaseModel


class Collection(BaseModel):
    id: Union[int, str]
    name: str
    description: Optional[str]
    slug: Optional[str]
    archived: Optional[bool]
    cards_id: Optional[List[int]] = []
    dashboards_id: Optional[List[int]] = []
    can_write: bool

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "can_write": self.can_write,
        }
