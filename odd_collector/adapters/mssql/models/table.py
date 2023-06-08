from dataclasses import asdict, dataclass, field

from funcy import omit

from odd_collector.adapters.mssql.models.column import Column
from odd_collector.helpers.bytes_to_str import convert_bytes_to_str_in_dict


@dataclass
class Table:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str
    columns: list[Column] = field(default_factory=list)

    @property
    def odd_metadata(self) -> dict[str, str]:
        values = omit(asdict(self), ["columns"])
        return convert_bytes_to_str_in_dict(values)
