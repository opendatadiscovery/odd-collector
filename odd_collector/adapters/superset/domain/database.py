from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class DatabaseConnectionParams:
    database: str
    host: str
    port: int


@dataclass
class Table:
    type: str
    name: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Table":
        return cls(
            type=data["type"],
            name=data["value"],
        )


@dataclass
class DatabaseSchema:
    schema_name: str
    tables: dict[str, Table] = field(default_factory=dict)


@dataclass
class Database:
    id: int
    database_name: str
    backend: str
    driver: str
    schemas: dict[str, DatabaseSchema] = field(default_factory=dict)
    connection_params: Optional[DatabaseConnectionParams] = None
    sqlalchemy_uri: Optional[str] = None
    parameters: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Database":
        return cls(
            id=data["id"],
            database_name=data["database_name"],
            backend=data["backend"],
            driver=data["driver"],
            sqlalchemy_uri=data.get("sqlalchemy_uri"),
            parameters=data.get("parameters"),
            schemas=data.get("schemas", []),
        )

    def create_generator() -> Any:
        pass
