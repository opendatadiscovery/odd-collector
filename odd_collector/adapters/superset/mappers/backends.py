from oddrn_generator.generators import (
    MysqlGenerator,
    PostgresqlGenerator,
    Generator,
    PrestoGenerator,
)
from typing import Dict, Type, List
from abc import abstractmethod
from odd_collector.adapters.superset.domain.database import Database


class DatabaseBackend:
    def __init__(self, database: Database):
        self.database = database

    database_backend: str
    generator_cls: Type[Generator]
    database_path_name: str
    schema_path_name: str
    table_path_name = "tables"

    def get_generator_for_database_lvl(self):
        return self.generator_cls(
            **{
                "host_settings": self.database.host,
                self.database_path_name: self.database.database_name,
            }
        )

    @abstractmethod
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        pass


class DeepLvlBackend(DatabaseBackend):
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        gen = self.get_generator_for_database_lvl()
        gen.get_oddrn_by_path(self.schema_path_name, schema_name)
        return gen


class ShallowLvlBackend(DatabaseBackend):
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        return self.get_generator_for_database_lvl()


class PostgresBackend(DeepLvlBackend):
    database_backend = "postgresql"
    generator_cls = PostgresqlGenerator
    database_path_name = "databases"
    schema_path_name = "schemas"


class MysqlBackend(ShallowLvlBackend):
    database_backend = "mysql"
    generator_cls = MysqlGenerator
    database_path_name = "databases"


class PrestoBackend(DeepLvlBackend):
    database_backend = "presto"
    generator_cls = PrestoGenerator
    database_path_name = "catalogs"
    schema_path_name = "schemas"


backends: List[Type[DatabaseBackend]] = [PostgresBackend, MysqlBackend, PrestoBackend]
backends_factory: Dict[str, Type[DatabaseBackend]] = {
    backend.database_backend: backend for backend in backends
}
