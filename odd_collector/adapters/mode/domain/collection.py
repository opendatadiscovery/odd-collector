from pydantic import BaseModel
from typing import Optional, Dict, Any
from ..generator import Generator


class Collection(BaseModel):
    token: str
    id: int
    space_type: str
    name: str
    description: str
    state: str
    restricted: bool
    free_default: str
    viewable: str
    links: dict

    viewed: Optional[str]
    default_access_level: Optional[str]

    @staticmethod
    def from_response(response: Dict[str, Any]):
        response["links"] = response.pop("_links")
        return Collection.parse_obj(response)

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("datasource", self.name)
        return oddrn_generator.get_oddrn_by_path("datasource")
