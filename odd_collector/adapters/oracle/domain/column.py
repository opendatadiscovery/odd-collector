from dataclasses import dataclass
from typing import Any, Optional

from funcy import omit

from .column_type import ColumnType


@dataclass
class Column:
    name: str
    type: ColumnType
    is_literal: Optional[bool]
    primary_key: Optional[bool]
    nullable: Optional[bool]
    default: Optional[Any]
    index: Optional[Any]
    unique: Optional[Any]
    comment: Optional[str]
    logical_type: Optional[str]

    @property
    def metadata(self):
        return omit(self, ["name", "type"])
