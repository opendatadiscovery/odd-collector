from abc import ABC, abstractmethod
from typing import Union

from psycopg2 import sql

from odd_collector.adapters.cockroachdb.connectors import CockroachDbSQLConnector


class AbstractRepository(ABC):
    @abstractmethod
    def get_tables(self) -> list[tuple]:
        pass

    @abstractmethod
    def get_columns(self) -> list[tuple]:
        pass

    @abstractmethod
    def get_primary_keys(self) -> list[tuple]:
        pass


class CockroachDbSQLRepository(AbstractRepository):
    def __init__(self, config):
        self.connector = CockroachDbSQLConnector(config)

    def get_columns(self) -> list[tuple]:
        return self.execute(self.column_metadata_query)

    def get_tables(self) -> list[tuple]:
        return self.execute(self.table_metadata_query)

    def get_primary_keys(self) -> list[tuple]:
        return self.execute(self.primary_key_query)

    def execute(self, query: Union[str, sql.Composed]) -> list[tuple]:
        with self.connector.connection() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    @property
    def primary_key_query(self):
        return """
            select c.relname, a.attname
            from pg_index i
                join pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                join pg_catalog.pg_class c on c.oid = a.attrelid
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where i.indisprimary
            and c.relkind in ('r', 'v')
            and a.attnum > 0
            and n.nspname not like 'pg_temp_%'
            and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
        """

    @property
    def table_metadata_query(self):
        return """
            select it.table_catalog
                , it.table_schema
                , it.table_name
                , it.table_type
                , it.is_insertable_into
                , iw.view_definition
                , iw.check_option                        as view_check_option
                , iw.is_updatable                        as view_is_updatable
                , iw.is_insertable_into                  as view_is_insertable_into
                , iw.is_trigger_updatable                as view_is_trigger_updatable
                , iw.is_trigger_deletable                as view_is_trigger_deletable
                , iw.is_trigger_insertable_into          as view_is_trigger_insertable_into
                , pg_catalog.pg_get_userbyid(c.relowner) as table_owner
                , c.reltuples                            as table_rows
                , pg_catalog.obj_description(c.oid)      as description
            from pg_catalog.pg_class c
                    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                    join information_schema.tables it on it.table_schema = n.nspname and it.table_name = c.relname
                    left join information_schema.views iw on iw.table_schema = n.nspname and iw.table_name = c.relname
            where c.relkind in ('r', 'v')
            and n.nspname not like 'pg_temp_%'
            and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
            order by n.nspname, c.relname;
        """

    @property
    def column_metadata_query(self):
        return """
            select ic.table_catalog
                , ic.table_schema
                , ic.table_name
                , ic.column_name
                , ic.ordinal_position
                , ic.column_default
                , ic.is_nullable
                , ic.data_type
                , ic.character_maximum_length
                , ic.character_octet_length
                , ic.numeric_precision
                , ic.numeric_precision_radix
                , ic.numeric_scale
                , ic.datetime_precision
                , ic.interval_type
                , ic.interval_precision
                , ic.character_set_catalog
                , ic.character_set_schema
                , ic.character_set_name
                , ic.collation_catalog
                , ic.collation_schema
                , ic.collation_name
                , ic.domain_catalog
                , ic.domain_schema
                , ic.domain_name
                , ic.udt_catalog
                , ic.udt_schema
                , ic.udt_name
                , ic.scope_catalog
                , ic.scope_schema
                , ic.scope_name
                , ic.maximum_cardinality
                , ic.dtd_identifier
                , ic.is_self_referencing
                , ic.is_identity
                , ic.identity_generation
                , ic.identity_start
                , ic.identity_increment
                , ic.identity_maximum
                , ic.identity_minimum
                , ic.identity_cycle
                , ic.is_generated
                , ic.generation_expression
                , ic.is_updatable
                , pg_catalog.col_description(c.oid, a.attnum) as description
            from pg_catalog.pg_attribute a
                    join pg_catalog.pg_class c on c.oid = a.attrelid
                    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                    join information_schema.columns ic on ic.table_schema = n.nspname and ic.table_name = c.relname and
                                                        ic.ordinal_position = a.attnum
            where c.relkind in ('r', 'v')
            and a.attnum > 0
            and n.nspname not like 'pg_temp_%'
            and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
            order by n.nspname, c.relname, a.attnum
        """
