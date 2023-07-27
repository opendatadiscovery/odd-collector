from dataclasses import dataclass, field
from typing import Any, Optional

from odd_models.models import MetadataExtension
from oddrn_generator import PostgresqlGenerator, SQLiteGenerator
from sqlalchemy.engine.url import make_url

from odd_collector.adapters.superset.domain.column import Column

from ..logger import logger
from .database import Database


@dataclass
class Dataset:
    id: int
    name: str
    database: Database
    kind: str
    schema: str
    columns: Optional[list[Column]] = field(default_factory=list)
    metadata: Optional[list[MetadataExtension]] = None
    owner: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, dataset: Any):
        return cls(
            id=dataset["id"],
            metadata=[],
            description=dataset.get("description"),
            name=dataset.get("table_name"),
            database=dataset.get("database"),
            kind=dataset.get("kind"),
            schema=dataset.get("schema"),
        )


class PostgresGeneratorAdaptee:
    def get_dataset_oddrn(self, dataset: Dataset) -> Optional[str]:
        try:
            database = dataset.database

            params = {
                "host_settings": database.parameters.get("host"),
                "databases": database.parameters.get("database"),
                "schemas": dataset.schema,
            }

            table_schema = dataset.schema
            table_name = dataset.name
            dataset_type = database.schemas[table_schema].tables[table_name].type
            if dataset_type not in ["view", "table"]:
                logger.warning(f"Dataset type {dataset_type} is not supported")
                return None

            dataset_type = "views" if dataset_type == "view" else "tables"
            params[dataset_type] = table_name

            generator = PostgresqlGenerator(**params)
            return generator.get_oddrn_by_path(dataset_type)
        except Exception as e:
            logger.warning(f"Failed to generate dataset oddrn: {e}")
            return None


class SqliteGeneratorAdaptee:
    def get_dataset_oddrn(self, dataset: Dataset) -> Optional[str]:
        try:
            database = dataset.database
            if not dataset.database.sqlalchemy_uri:
                raise AttributeError("Sqlite database uri must be set")

            table_schema = dataset.schema
            table_name = dataset.name

            url = make_url(database.sqlalchemy_uri)
            params = {
                "path": url.database,
            }
            dataset_type = database.schemas[table_schema].tables[table_name].type
            if dataset_type not in ["view", "table"]:
                logger.warning(f"Dataset type {dataset_type} is not supported")
                return None

            dataset_type = "views" if dataset_type == "view" else "tables"
            params[dataset_type] = table_name

            generator = SQLiteGenerator(**params)

            return generator.get_oddrn_by_path(dataset_type)
        except Exception as e:
            logger.warning(f"Failed to generate dataset oddrn: {e}")
            return None


SUPPORTED_BACKENDS = {
    "postgresql": PostgresGeneratorAdaptee,
    "sqlite": SqliteGeneratorAdaptee,
}


def create_dataset_oddrn(dataset: Dataset) -> Optional[str]:
    database = dataset.database
    backend = SUPPORTED_BACKENDS.get(database.backend)

    if not backend:
        logger.warning(
            f"Database backend {database.backend} is not supported for generating dataset oddrn"
        )
        return None

    return backend().get_dataset_oddrn(dataset)
