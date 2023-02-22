from typing import Any, Dict

from funcy import omit
from pydantic import BaseModel


class Column(BaseModel):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_precision_radix: Any
    numeric_scale: Any
    collation_name: Any
    is_identity: Any
    identity_generation: Any
    identity_start: Any
    identity_increment: Any
    identity_cycle: Any
    comment: Any
    is_primary_key: Any
    is_clustering_key: Any

    @property
    def metadata(self) -> Dict[str, Any]:
        excluded = [
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "column_default",
            "is_nullable",
            "data_type",
        ]
        return omit(self.__dict__, excluded)
