from abc import abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Optional, Union

from funcy import first, get_lax

from .connection_params import ConnectionParams
from .table import EmbeddedTable, SnowflakeTable


@dataclass
class EmbeddedDatabase:
    id: str
    name: str
    connection_type: str
    owner: Optional[str]
    tables: list = field(default_factory=list)

    @classmethod
    def from_dict(cls, **kwargs):
        tables = []
        for table in kwargs.get("tables", []):
            table = EmbeddedTable.from_dict(
                **dict(
                    id=table["id"],
                    name=table["name"],
                    db_id=kwargs["id"],
                    db_name=kwargs["name"],
                    connection_type=kwargs["connectionType"],
                    schema=table["schema"],
                    columns=table["columns"],
                )
            )
            tables.append(table)

        return cls(
            id=kwargs["id"],
            name=kwargs["name"],
            connection_type=kwargs["connectionType"],
            owner=get_lax(first(kwargs["downstreamOwners"]), "name"),
            tables=tables,
        )

    @property
    def is_embedded(self):
        return True


@dataclass
class ExternalDatabase:
    _CONNECTION_TYPE: ClassVar[tuple[str]]

    id: str
    name: str
    connection_params: ConnectionParams
    connection_type: str
    tables: list = field(default_factory=list)

    def __new__(cls, *args, **kwargs):
        for subclass in cls.__subclasses__():
            if kwargs["connection_type"].lower() in subclass._CONNECTION_TYPE:
                return super().__new__(subclass)

        raise NotImplementedError(
            f"Database {kwargs['connection_type']}  is not supported"
        )

    def __post_init__(self):
        self.tables = [self.create_table(**table) for table in self.tables]

    @abstractmethod
    def create_table(self, **data) -> Union[EmbeddedTable, SnowflakeTable]:
        raise NotImplementedError


@dataclass
class SnowflakeDatabase(ExternalDatabase):
    _CONNECTION_TYPE: ClassVar[tuple[str]] = ("snowflake",)

    def create_table(self, **data) -> SnowflakeTable:
        return SnowflakeTable(
            id=data["id"],
            host=self.connection_params.host,
            database=self.connection_params.name,
            name=data["name"],
            connection_type="snowflake",
            schema=data["schema"],
        )
