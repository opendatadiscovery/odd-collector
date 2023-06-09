from dataclasses import dataclass
from typing import Any, Optional

import sqlalchemy.sql.sqltypes as sqltype
from funcy import omit


@dataclass
class Column:
    name: str
    type: sqltype
    primary_key: Optional[bool]
    nullable: Optional[bool]
    default: Optional[Any]
    autoincrement: Optional[Any]
    logical_type: Optional[str]

    @property
    def odd_metadata(self) -> dict:
        return omit(self.__dict__, {"name", "type", "logical_type"})
