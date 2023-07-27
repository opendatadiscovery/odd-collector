from dataclasses import dataclass


@dataclass
class Column:
    id: int
    name: str
    remote_type: str
