from oddrn_generator.generators import MysqlGenerator, PostgresqlGenerator, Generator
from typing import Dict, Type, List
from abc import abstractmethod
from odd_collector.adapters.superset.domain.database import Database


class DatabaseBackend:
    def __init__(self, database: Database):
        self.database = database

    database_backend: str
    generator_cls: Type[Generator]
    table_path_name: str

    @abstractmethod
    def get_generator(self) -> Generator:
        pass

    @abstractmethod
    def get_generator_with_schemas(self, sche) -> Generator:
        pass


class JdbcBackend(DatabaseBackend):
    table_path_name = "tables"

    def get_generator(self):
        return self.generator_cls(
            host_settings=self.database.host,
            # host_settings='localhost',
            databases=self.database.database_name,
        )

    @abstractmethod
    def get_generator_with_schemas(self, schema_name: str) -> Generator:
        pass


class PostgresBackend(JdbcBackend):
    database_backend = "postgresql"
    generator_cls = PostgresqlGenerator

    def get_generator_with_schemas(self, schema_name: str) -> Generator:
        gen = self.get_generator()
        gen.get_oddrn_by_path("schemas", schema_name)
        return gen


class MysqlBackend(JdbcBackend):
    database_backend = "mysql"
    generator_cls = MysqlGenerator

    def get_generator_with_schemas(self, sche) -> Generator:
        return self.get_generator()


backends: List[Type[DatabaseBackend]] = [PostgresBackend, MysqlBackend]
backends_factory: Dict[str, Type[DatabaseBackend]] = {
    backend.database_backend: backend for backend in backends
}
