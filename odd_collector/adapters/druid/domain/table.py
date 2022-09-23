import dataclasses


@dataclasses.dataclass
class Table:
    catalog: str
    schema: str
    name: str
    type: str
    is_joinable: str
    is_broadcast: str
