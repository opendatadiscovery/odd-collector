from dataclasses import dataclass, field
from typing import Optional, Union

from oddrn_generator import (
    BigQueryStorageGenerator,
    RedshiftGenerator,
    SnowflakeGenerator,
)

from .column import Column


@dataclass
class EmbeddedTable:
    id: str
    name: str
    db_id: str
    db_name: str
    connection_type: str
    schema: Optional[str] = field(default="unknown_schema")
    columns: Optional[list[Column]] = field(default_factory=list)
    owners: Optional[list[str]] = field(default_factory=list)
    description: Optional[str] = field(default=None)

    @property
    def is_embedded(self):
        return True

    @classmethod
    def from_dict(cls, **kwargs):
        columns = [
            Column.from_dict(**response) for response in kwargs.get("columns", [])
        ]

        return cls(
            id=kwargs["id"],
            name=kwargs["name"],
            db_id=kwargs["db_id"],
            db_name=kwargs["db_name"],
            connection_type=kwargs["connection_type"],
            schema=kwargs["schema"],
            columns=columns,
            owners=kwargs.get("owners"),
            description=kwargs.get("description"),
        )


@dataclass
class ExternalTable:
    id: str
    name: str
    connection_type: str

    @property
    def is_embedded(self):
        return False

    @classmethod
    def from_dict(cls, **kwargs) -> "ExternalTable":
        raise NotImplementedError


@dataclass
class SnowflakeTable(ExternalTable):
    id: str
    host: str
    database: str
    name: str
    schema: str

    def get_oddrn(self):
        suffix = ".snowflakecomputing.com"
        host = self.host.split(suffix)[0].upper() + suffix

        generator = SnowflakeGenerator(
            host_settings=host,
            databases=self.database.upper(),
            schemas=self.schema.upper(),
            tables=self.name.upper(),
        )
        return generator.get_oddrn_by_path("databases")


@dataclass
class RedshiftTable(ExternalTable):
    host: str
    database: str
    name: str
    schema: str

    def get_oddrn(self):
        generator = RedshiftGenerator(
            host_settings=self.host,
            databases=self.database,
            schemas=self.schema,
            tables=self.name,
        )

        return generator.get_oddrn_by_path("tables")


@dataclass
class BigqueryTable(ExternalTable):
    database: str
    name: str
    schema: str

    def get_oddrn(self):
        db_name = self.database.lower()
        schema = self.schema
        name = self.name

        return BigQueryStorageGenerator(
            google_cloud_settings={"project": db_name},
            datasets=schema,
            tables=name,
        ).get_oddrn_by_path("tables")


Table = Union[EmbeddedTable, ExternalTable]
