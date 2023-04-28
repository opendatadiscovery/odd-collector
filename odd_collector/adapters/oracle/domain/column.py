from dataclasses import dataclass
from typing import Any, Optional

from funcy import omit

import sqlalchemy.sql.sqltypes as sqltype


@dataclass
class Column:
    name: str
    type: sqltype
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
