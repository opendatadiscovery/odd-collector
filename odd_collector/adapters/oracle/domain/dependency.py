import dataclasses
import enum


class DependencyType(enum.Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"


@dataclasses.dataclass
class Dependency:
    name: str
    referenced_owner: str
    referenced_name: str
    referenced_type: DependencyType
