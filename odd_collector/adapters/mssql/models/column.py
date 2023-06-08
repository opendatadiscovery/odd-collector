from dataclasses import asdict, dataclass
from typing import Dict

from odd_collector.helpers.bytes_to_str import convert_bytes_to_str_in_dict


@dataclass
class Column:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    ordinal_position: int
    column_default: str
    is_nullable: bool
    is_primary_key: bool
    data_type: str
    character_maximum_length: int
    character_octet_length: int
    numeric_precision: str
    numeric_precision_radix: str
    numeric_scale: str
    datetime_precision: str
    character_set_catalog: str
    character_set_schema: str
    character_set_name: str
    collation_catalog: str
    collation_schema: str
    collation_name: str
    domain_catalog: str
    domain_schema: str
    domain_name: str

    @property
    def odd_metadata(self) -> Dict[str, str]:
        return convert_bytes_to_str_in_dict(asdict(self))

    def __post_init__(self):
        self.is_primary_key = bool(self.is_primary_key)
