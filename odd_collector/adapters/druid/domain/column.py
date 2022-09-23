from dataclasses import dataclass


@dataclass
class Column:
    catalog: str
    schema: str
    table: str
    name: str
    type: str
    is_nullable: bool
