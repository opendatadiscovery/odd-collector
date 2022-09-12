from odd_collector.domain.plugin import PrestoPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter

from typing import NamedTuple, Any, Type, Optional, List
from prestodb.dbapi import connect
from pandas import DataFrame
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.generators import Generator, HostnameModel
from odd_models.models import (
    DataEntity,
    DataSet,
    DataEntityType,
    DataEntityGroup,
    DataSetField,
    Type as ColumnType,
    DataSetFieldType,
    DataEntityList
)


class PrestoPathsModel(BasePathsModel):
    catalogs: Optional[str]
    schemas: Optional[str]
    tables: Optional[str]
    columns: Optional[str]

    class Config:
        dependencies_map = {
            "catalogs": (
                "catalogs",
            ),

            "schemas": ("catalogs", "schemas"),
            "tables": ("catalogs", "schemas", "tables"),
            "columns": ("catalogs", "schemas", "tables", "columns"),
        }
        # data_source_path = "databases"


class PrestoGenerator(Generator):
    source = "presto"
    paths_model = PrestoPathsModel
    server_model = HostnameModel


# mappers.models
class ColumnMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    column_name: Any
    data_type: Any
    type_name: Any
    column_size: Any
    buffer_length: Any
    decimal_digits: Any
    num_prec_radix: Any
    nullable: Any
    remarks: Any
    column_def: Any
    sql_data_type: Any
    sql_datetime_sub: Any
    char_octet_length: Any
    ordinal_position: Any
    is_nullable: Any
    scope_catalog: Any
    scope_schema: Any
    scope_table: Any
    source_data_type: Any
    is_autoincrement: Any
    is_generatedcolumn: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join(cls._fields)


# mappers.table


# repository
class PrestoRepository:
    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__user = config.user

    @property
    def server_url(self):
        return f"{self.__host}:{self.__port}"

    def __execute(self, query: str) -> List[list]:
        presto_conn_params = {
            "host": self.__host,
            "port": self.__port,
            "user": self.__user,
        }
        with connect(**presto_conn_params) as conn:
            cur = conn.cursor()
            cur.execute(query)
            records = cur.fetchall()
            return records

    @property
    def __columns_query(self):
        return """
            SELECT *
            FROM system.jdbc.columns 
            WHERE table_cat != 'system' 
            AND table_schem NOT IN ('information_schema', 'sys')

        """

    def get_columns(self) -> List[list]:
        return self.__execute(self.__columns_query)


class Column:
    def __init__(self, name: str, table_name: str = None, schema_name: str = None, catalog_name: str = None):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.name = name


class Table:
    def __init__(self, name, schema_name: str = None, catalog_name: str = None):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.name = name


class Schema:
    def __init__(self, name: str, catalog_name: str = None, tables: List[Table] = None):
        self.tables = tables
        self.name = name
        self.catalog_name = catalog_name


class Catalog:
    def __init__(self, name: str, schemas: List[Schema] = None):
        self.schemas = schemas
        self.name = name


class Adapter(AbstractAdapter):
    def __init__(
            self, config: PrestoPlugin, repository: Type[PrestoRepository] = None
    ) -> None:
        repository = repository or PrestoRepository
        self.repository = repository(config)

        self.__oddrn_generator = PrestoGenerator(
            host_settings=self.repository.server_url
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        columns_nodes = self.repository.get_columns()
        cols_meta = [ColumnMetadata(*c) for c in columns_nodes]
        df = DataFrame(columns_nodes)
        df.columns = ColumnMetadata._fields
        out = {a: {k: f.groupby('table_name')['column_name'].apply(list).to_dict()
                   for k, f in g.groupby('table_schem')}
               for a, g in df.groupby('table_cat')}

        cats_entities = [DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("catalogs", catalog_node_name),
            name=catalog_node_name,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[self.__oddrn_generator.get_oddrn_by_path("schemas", schema_node_name) for
                               schema_node_name in
                               schemas_node.keys()
                               ]
            ),
        ) for catalog_node_name, schemas_node in out.items()]

        schemas_entities: List[DataEntity] = []

        for catalog_node_name, schemas_node in out.items():
            self.__oddrn_generator.set_oddrn_paths(catalogs=catalog_node_name)
            for schema_node_name, tables_node in schemas_node.items():
                data_entity = DataEntity(
                    oddrn=self.__oddrn_generator.get_oddrn_by_path("schemas", schema_node_name),
                    name=schema_node_name,
                    type=DataEntityType.DATABASE_SERVICE,
                    metadata=[],
                    data_entity_group=DataEntityGroup(
                        entities_list=[self.__oddrn_generator.get_oddrn_by_path("tables", table_node_name) for
                                       table_node_name
                                       in
                                       tables_node.keys()
                                       ]
                    ),
                )
                schemas_entities.append(data_entity)

        tables_entities: List[DataEntity] = []
        for catalog_node_name, schemas_node in out.items():
            for schema_node_name, tables_node in schemas_node.items():
                self.__oddrn_generator.set_oddrn_paths(catalogs=catalog_node_name, schemas=schema_node_name)
                for table_node_name, columns in tables_node.items():
                    data_entity = DataEntity(
                        oddrn=self.__oddrn_generator.get_oddrn_by_path("tables", table_node_name),
                        name=table_node_name,
                        type=DataEntityType.TABLE,
                        metadata=[],
                        dataset=DataSet(
                            parent_oddrn=self.__oddrn_generator.get_oddrn_by_path("tables"),
                            field_list=[DataSetField(oddrn=self.__oddrn_generator.get_oddrn_by_path("columns", column),
                                                     name=column,
                                                     type=DataSetFieldType(
                                                         type=ColumnType.TYPE_UNKNOWN,
                                                         is_nullable=False
                                                     )
                                                     )
                                        for column in columns],
                        )
                    )
                    tables_entities.append(data_entity)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*cats_entities, *schemas_entities, *tables_entities],
        )


# presto_plugin = PrestoPlugin(type="presto", host='localhost',
#                              port=8081, user='presto',
#                              name='presto_adapter'
#                              )
# ad = Adapter(presto_plugin)
# ent = ad.get_data_entity_list()
pass
