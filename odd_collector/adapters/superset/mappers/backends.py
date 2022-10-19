from typing import Dict, Type, List
from odd_collector.adapters.superset.domain.database import Database
from oddrn_generator.utils.external_generators import ExternalPostgresGenerator, ExternalPrestoGenerator, \
    ExternalMysqlGenerator, ExternalDbSettings, ExternalGeneratorBuilder, ExternalMssqlGenerator, \
    ExternalTrinoGenerator


class SupersetExternalGeneratorBuilder(ExternalGeneratorBuilder):
    def __init__(self, database: Database):
        self.database = database

    def build_db_settings(self) -> ExternalDbSettings:
        return ExternalDbSettings(host=self.database.host,
                                  database_name=self.database.database_name)


class PostgresBackend(SupersetExternalGeneratorBuilder):
    external_generator = ExternalPostgresGenerator
    type = "postgresql"


class MysqlBackend(SupersetExternalGeneratorBuilder):
    external_generator = ExternalMysqlGenerator
    type = "mysql"


class PrestoBackend(SupersetExternalGeneratorBuilder):
    external_generator = ExternalPrestoGenerator
    type = "presto"


class TrinoBackend(SupersetExternalGeneratorBuilder):
    external_generator = ExternalTrinoGenerator
    type = "trino"


class MssqlBackend(SupersetExternalGeneratorBuilder):
    external_generator = ExternalMssqlGenerator
    type = "mssql"


backends: List[Type[SupersetExternalGeneratorBuilder]] = [
    PostgresBackend,
    MysqlBackend,
    TrinoBackend,
    PrestoBackend,
    MssqlBackend

]

backends_factory: Dict[str, Type[SupersetExternalGeneratorBuilder]] = {
    backend.type: backend for backend in backends
}
