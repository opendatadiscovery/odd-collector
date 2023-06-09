from dataclasses import dataclass
from typing import List

from funcy import omit

from .column import Column


@dataclass
class Table:
    name: str
    columns: List[Column]

    @property
    def odd_metadata(self) -> dict:
        return omit(self.__dict__, {"columns"})
