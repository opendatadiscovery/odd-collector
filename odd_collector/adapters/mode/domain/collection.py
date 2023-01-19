from dataclasses import dataclass
from typing import Optional, Dict, Any
from ..generator import Generator


@dataclass
class Collection:
    token: str
    id: int
    space_type: str
    name: str
    description: str
    state: str
    restricted: bool
    free_default: str
    viewable: str
    _links: dict

    viewed: Optional[str]
    default_access_level: Optional[str]

    @staticmethod
    def from_response(response: Dict[str, Any]):
        collection = Collection(
            token=response.get("token"),
            id=response.get("id"),
            space_type=response.get("space_type"),
            name=response.get("name"),
            description=response.get("description"),
            state=response.get("state"),
            restricted=response.get("restricted"),
            free_default=response.get("free_default"),
            viewable=response.get("viewable"),
            _links=response.get("_links"),
            viewed=response.get("viewed"),
            default_access_level=response.get("default_access_level"),
        )
        return collection

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("datasource", self.name)
        return oddrn_generator.get_oddrn_by_path("datasource")
