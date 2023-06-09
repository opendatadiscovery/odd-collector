from dataclasses import dataclass, field
from typing import Any

from odd_collector.helpers.datetime import Datetime
from odd_collector.models import Table, Column


@dataclass(frozen=True)
class DuckDBColumn(Column):
    odd_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass()
class DuckDBTable(Table):
    create_time: Datetime = None
    update_time: Datetime = None
    columns: list[DuckDBColumn] = field(default_factory=list)
    odd_metadata: dict[str, Any] = field(default_factory=dict)
