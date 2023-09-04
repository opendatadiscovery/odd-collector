import dataclasses
from typing import Any, Optional

from odd_collector_sdk.utils.metadata import HasMetadata

from odd_collector.helpers.datetime import Datetime

from .column import Column


@dataclasses.dataclass
class Table(HasMetadata):
    catalog: str
    schema: str
    name: str
    type: str
    create_time: Datetime
    update_time: Datetime
    table_rows: Optional[int] = None
    comment: Optional[str] = None
    sql_definition: Optional[str] = None
    columns: list["Column"] = dataclasses.field(default_factory=list)
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def odd_metadata(self):
        return self.metadata
