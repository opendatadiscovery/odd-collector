from typing import Type, List, Dict
from oddrn_generator.utils.external_generators import ExternalPostgresGenerator, ExternalMssqlGenerator, \
    ExternalMysqlGenerator, ExternalDbSettings, ExternalGeneratorBuilder
from odd_collector.adapters.redash.domain.datasource import DataSource


class RedashExternalGeneratorBuilder(ExternalGeneratorBuilder):
    def __init__(self, datasource: DataSource):
        self.datasource = datasource

    host_key: str
    port_key: str
    db_name_key: str

    def build_db_settings(self) -> ExternalDbSettings:
        return ExternalDbSettings(host=self.datasource.options[self.host_key],
                                  database_name=self.datasource.options[self.db_name_key])


class PostgresType(RedashExternalGeneratorBuilder):
    external_generator = ExternalPostgresGenerator
    type = "pg"
    host_key = "host"
    port_key = "port"
    db_name_key = "dbname"


class MssqlType(RedashExternalGeneratorBuilder):
    external_generator = ExternalMssqlGenerator
    type = "mssql"
    host_key = "server"
    port_key = "port"
    db_name_key = "db"


class MysqlType(RedashExternalGeneratorBuilder):
    external_generator = ExternalMysqlGenerator
    type = "mysql"
    host_key = "host"
    port_key = "port"
    db_name_key = "db"


class RdsMysqlType(MysqlType):
    type = "rds_mysql"


ds_types: List[Type[RedashExternalGeneratorBuilder]] = [
    PostgresType,
    MssqlType,
    MysqlType,
    RdsMysqlType

]

ds_types_factory: Dict[str, Type[RedashExternalGeneratorBuilder]] = {
    ds.type: ds for ds in ds_types
}
