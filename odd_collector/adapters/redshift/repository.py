from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from psycopg2 import sql
from psycopg2.extensions import AsIs

from odd_collector.adapters.redshift.connector import RedshiftConnector
from odd_collector.adapters.redshift.logger import logger
from odd_collector.adapters.redshift.mappers.metadata import (
    MetadataColumns,
    MetadataSchemas,
    MetadataTables,
)
from odd_collector.domain.plugin import RedshiftPlugin


class AbstractRepository(ABC):
    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_columns(self):
        pass


class RedshiftRepository(AbstractRepository):
    def __init__(self, config: RedshiftPlugin):
        self.__redshift_connector = RedshiftConnector(config)
        self.__schemas = config.schemas
        self._dbname = config.database

        logger.debug(f"Start fetching data for database {self._dbname}")
        logger.debug(f'Schemas for filter: {self.__schemas or "Were not set"}')

    def get_schemas(self) -> MetadataSchemas:
        return MetadataSchemas(
            self.__execute(self.metadata_schemas_base_query(self.__schemas)),
            self.__execute(self.metadata_schemas_redshift_query(self.__schemas)),
            self.__execute(self.metadata_schemas_external_query(self.__schemas)),
        )

    def get_tables(self) -> MetadataTables:
        return MetadataTables(
            self.__execute(self.metadata_tables_base_query(self.__schemas)),
            self.__execute(self.metadata_tables_all_query(self.__schemas)),
            self.__execute(self.metadata_tables_redshift_query(self.__schemas)),
            self.__execute(self.metadata_tables_external_query(self.__schemas)),
            self.__execute(self.metadata_tables_info_query(self.__schemas)),
        )

    def get_columns(self) -> MetadataColumns:
        return MetadataColumns(
            self.__execute(self.metadata_columns_base_query(self.__schemas)),
            self.__execute(self.metadata_columns_redshift_query(self.__schemas)),
            self.__execute(self.metadata_columns_external_query(self.__schemas)),
        )

    def get_primary_keys(self) -> list[tuple]:
        return self.__execute(self.primary_keys_query)

    def __execute(self, query: Union[str, sql.Composed]) -> list[tuple]:
        with self.__redshift_connector.connection() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def filter_schemas_params(schemas: Optional[list[str]]) -> dict[str, Any]:
        predicate = "in" if schemas else "not in"
        schemas = schemas or [
            "pg_toast",
            "pg_internal",
            "catalog_history",
            "pg_catalog",
            "information_schema",
        ]

        return {
            "predicate": sql.Literal(AsIs(predicate)),
            "schemas": sql.Literal(AsIs(", ".join(f"'{s}'" for s in schemas))),
        }

    def metadata_schemas_base_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                database_name, schema_name, schema_owner, schema_type, schema_acl, source_database, schema_option
            from
                pg_catalog.svv_all_schemas
            where schema_name {predicate} ( {schemas} )
                and schema_name not like 'pg_temp_%'
                and database_name = current_database()
            order by
                database_name, schema_name
        """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_schemas_redshift_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                database_name, schema_name, schema_owner, schema_type, schema_acl, schema_option
            from
                pg_catalog.svv_redshift_schemas
            where
                schema_name {predicate} ( {schemas} )
                and schema_name not like 'pg_temp_%'
                and database_name = current_database()
            order by
                database_name, schema_name
    """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_schemas_external_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                databasename, schemaname, esoid, eskind, esowner, esoptions
            from
                pg_catalog.svv_external_schemas
            where
                schemaname {predicate} ({schemas})
                and schemaname not like 'pg_temp_%'
                and databasename = CURRENT_DATABASE()
            order by
                databasename, schemaname
        """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_tables_base_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                table_catalog, table_schema, table_name, table_type, remarks
            from
                pg_catalog.svv_tables
            where table_schema {predicate} ( {schemas} )
                and table_schema not like 'pg_temp_%'
                and table_type in ('BASE TABLE', 'VIEW')
                and table_catalog = current_database()
            order by
                table_catalog, table_schema, table_name
        """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_tables_all_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                 (current_database())::character varying(128)             as database_name
                 , n.nspname                                                as schema_name
                 , c.relname                                                as table_name
                 , case when c.relkind = 'v' then 'VIEW' else 'TABLE' end   as table_type
                 , pg_get_userbyid(c.relowner)                              as table_owner
                 , relcreationtime                                          as table_creation_time
                 , case
                       when relkind = 'v' then
                           coalesce(pg_get_viewdef(c.reloid, true), '') end as view_ddl
            from
                pg_catalog.pg_class_info c
            left join
                pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where
                c.relkind in ('r', 'v')
                and n.nspname not like 'pg_temp_%'
                and n.nspname {predicate} ( {schemas} )
            order by
                n.nspname, c.relname
            """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_tables_redshift_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                database_name, schema_name, table_name, table_type, table_acl, remarks
            from
                pg_catalog.svv_redshift_tables
            where
                schema_name {predicate} ( {schemas} )
                and schema_name not like 'pg_temp_%'
                and database_name = current_database()
            order by
                database_name, schema_name, table_name
    """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_tables_external_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                (current_database())::character varying(128) as databasename,
                schemaname, tablename, location,
                input_format, output_format, serialization_lib,
                serde_parameters, compressed, parameters, tabletype
            from
                pg_catalog.svv_external_tables
            where
                schemaname {predicate} ({schemas})
                and schemaname not like 'pg_temp_%'
            order by
                schemaname, tablename
        """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_tables_info_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                database, schema, table_id, "table", encoded, diststyle,
                sortkey1, max_varchar, trim(sortkey1_enc) sortkey1_enc, sortkey_num,
                size, pct_used, empty, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows, estimated_visible_rows,
                risk_event, vacuum_sort_benefit
            from
                pg_catalog.svv_table_info
            where
                schema {predicate} ({schemas})
                and schema not like 'pg_temp_%'
                and database = current_database()
            order by
                database, schema, "table"
            """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_columns_base_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                c.database_name, c.schema_name, c.table_name, c.column_name,
                c.ordinal_position, c.column_default, c.is_nullable, c.data_type,
                c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.remarks
            from
                pg_catalog.svv_all_columns as c
            join
                pg_catalog.svv_tables as t on (
                    t.table_catalog = c.database_name and t.table_schema = c.schema_name and t.table_name = c.table_name
                )
            where
                c.schema_name {predicate} ({schemas})
                and c.schema_name not like 'pg_temp_%'
                and t.table_type in ('BASE TABLE', 'VIEW')
                and c.database_name = current_database()
            order by
                c.database_name, c.schema_name, c.table_name, c.ordinal_position
            """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_columns_redshift_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                database_name, schema_name, table_name, column_name,
                ordinal_position, data_type, column_default, is_nullable,
                encoding, distkey, sortkey, column_acl, remarks
            from
                pg_catalog.svv_redshift_columns
            where
                schema_name {predicate} ({schemas})
                and schema_name not like 'pg_temp_%'
                and database_name = current_database()
            order by
                database_name, schema_name, table_name, ordinal_position
            """
        ).format(**self.filter_schemas_params(schemas))

    def metadata_columns_external_query(self, schemas: Optional[list[str]]):
        return sql.SQL(
            """
            select
                (current_database())::character varying(128) as databasename,
                schemaname, tablename, columnname, external_type, columnnum, part_key, is_nullable
            from
                pg_catalog.svv_external_columns
            where
                schemaname {predicate} ({schemas})
                and schemaname not like 'pg_temp_%'
            order by
                schemaname, tablename, columnnum
        """
        ).format(**self.filter_schemas_params(schemas))

    @property
    def primary_keys_query(self):
        return """
                select 
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.column_name
                from information_schema.table_constraints tco
                join information_schema.key_column_usage kcu 
                    on kcu.constraint_name = tco.constraint_name
                    and kcu.constraint_schema = tco.constraint_schema
                    and kcu.constraint_name = tco.constraint_name
                where tco.constraint_type = 'PRIMARY KEY'
                order by tco.constraint_schema,
                         tco.constraint_name,
                         kcu.ordinal_position;
            """
