from datetime import datetime
from typing import Any, Dict, List, Optional

from funcy import omit

from .column import Column
from .entity import Connectable


class Table(Connectable):
    table_catalog: str
    table_schema: str
    table_name: str
    table_owner: Optional[str]
    table_type: str
    is_transient: Optional[str]
    clustering_key: Optional[Any]
    row_count: Optional[int]
    retention_time: Optional[int]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    table_comment: Optional[str]
    self_referencing_column_name: Optional[str]
    reference_generation: Optional[str]
    user_defined_type_catalog: Optional[str]
    user_defined_type_schema: Optional[str]
    user_defined_type_name: Optional[str]
    is_insertable_into: Optional[str]
    is_typed: Optional[str]
    columns: Optional[List[Column]] = []

    @property
    def metadata(self) -> Dict[str, Any]:
        exclude = [
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "table_owner",
            "row_count",
            "comment",
            "last_altered",
            "created",
            "upstream",
            "downstream",
        ]

        return omit(self.__dict__, exclude)
