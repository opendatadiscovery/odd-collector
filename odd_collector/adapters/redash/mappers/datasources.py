from abc import abstractmethod
from ..domain.datasource import DataSource
from typing import Type, List, Dict
from oddrn_generator import (
    Generator,
    PostgresqlGenerator,
    MssqlGenerator,
    MysqlGenerator,
)


class DataSourceType:
    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    type: str
    generator_cls: Type[Generator]
    database_path_name: str
    schema_path_name: str
    table_path_name = "tables"
    host_key: str
    port_key: str
    db_name_key: str

    def get_generator_for_database_lvl(self):
        return self.generator_cls(
            **{
                "host_settings": self.data_source.options[self.host_key],
                self.database_path_name: self.data_source.options[self.db_name_key],
            }
        )

    @abstractmethod
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        pass

    def get_generator_for_table_lvl(
        self, schema_name: str, table_name: str
    ) -> Generator:
        gen = self.get_generator_for_schema_lvl(schema_name)
        gen.get_oddrn_by_path(self.table_path_name, table_name)
        return gen


class DeepLvlType(DataSourceType):
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        gen = self.get_generator_for_database_lvl()
        gen.get_oddrn_by_path(self.schema_path_name, schema_name)
        return gen


class ShallowLvlType(DataSourceType):
    def get_generator_for_schema_lvl(self, schema_name: str) -> Generator:
        return self.get_generator_for_database_lvl()


class PostgresType(DeepLvlType):
    type = "pg"
    generator_cls = PostgresqlGenerator
    database_path_name = "databases"
    schema_path_name = "schemas"
    host_key = "host"
    port_key = "port"
    db_name_key = "dbname"


class MssqlType(DeepLvlType):
    type = "mssql"
    generator_cls = MssqlGenerator
    database_path_name = "databases"
    schema_path_name = "schemas"
    host_key = "server"
    port_key = "port"
    db_name_key = "db"


class MysqlType(ShallowLvlType):
    type = "mysql"
    generator_cls = MysqlGenerator
    database_path_name = "databases"
    host_key = "host"
    port_key = "port"
    db_name_key = "db"


class MysqlRdsType(MysqlType):
    type = "rds_mysql"


ds_types: List[Type[DataSourceType]] = [
    PostgresType,
    MysqlType,
    MysqlRdsType,
    MssqlType,
]
ds_types_factory: Dict[str, Type[DataSourceType]] = {
    ds_type.type: ds_type for ds_type in ds_types
}
