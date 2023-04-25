from dataclasses import dataclass
from typing import Dict, Any

from funcy import omit


@dataclass(frozen=True)
class Collection:
    bucket: str
    datastore_id: str
    id: str
    name: str
    namespace: str
    namespace_id: str
    path: str
    scope: str
    columns: Dict[str, Dict[str, Any]]

    @property
    def odd_metadata(self) -> dict:
        return omit(
            self.__dict__,
            {
                "datastore_id",
                "id",
                "namespace_id",
                "path",
            },
        )
