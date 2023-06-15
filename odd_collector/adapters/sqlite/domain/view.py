from dataclasses import dataclass
from typing import List

from funcy import omit

from .column import Column


@dataclass
class View:
    name: str
    columns: List[Column]
    view_definition: str

    @property
    def odd_metadata(self) -> dict:
        return omit(self.__dict__, {"view_definition", "columns"})
