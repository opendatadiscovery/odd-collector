import contextlib
from abc import ABC, abstractmethod
from typing import Callable, List, Type, Union

from funcy import lsplit
from odd_collector_sdk.errors import DataSourceError
from snowflake import connector
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import DataError, ProgrammingError

from odd_collector.domain.plugin import SnowflakePlugin
from odd_collector.helpers import LowerKeyDict

from .domain import Column, Pipe, RawPipe, RawStage, Table, View

TABLES_VIEWS_QUERY = """
with recursive cte as (
    select
        referenced_database,
        referenced_schema,
        referenced_object_name,
        referenced_object_id,
        referenced_object_domain,
        referencing_database,
        referencing_schema,
        referencing_object_name,
        referencing_object_id,
        referencing_object_domain
    from snowflake.account_usage.object_dependencies
    union all
    select
        deps.referenced_database,
        deps.referenced_schema,
        deps.referenced_object_name,
        deps.referenced_object_id,
        deps.referenced_object_domain,
        deps.referencing_database,
        deps.referencing_schema,
        deps.referencing_object_name,
        deps.referencing_object_id,
        deps.referencing_object_domain
    from snowflake.account_usage.object_dependencies deps
    join cte
        on deps.referencing_object_id = cte.referenced_object_id
        and deps.referencing_object_domain = cte.referenced_object_domain
),
upstream as (
    select
        referencing_database as node_database,
        referencing_schema as node_schema,
        referencing_object_name as node_name,
        referencing_object_domain as node_domain,
        array_agg(
            distinct concat_ws(
                '.',
                referenced_database,
                referenced_schema,
                referenced_object_name,
                referenced_object_domain
            )
        ) as nodes
    from cte
    group by
        referencing_database,
        referencing_schema,
        referencing_object_name,
        referencing_object_id,
        referencing_object_domain
),
downstream as (
    select
        referenced_database as node_database,
        referenced_schema as node_schema,
        referenced_object_name as node_name,
        referenced_object_domain as node_domain,
        array_agg(
            distinct concat_ws(
                '.',
                referencing_database,
                referencing_schema,
                referencing_object_name,
                referencing_object_domain
            )
        ) as nodes
    from cte
    group by referenced_database,
        referenced_schema,
        referenced_object_name,
        referenced_object_id,
        referenced_object_domain
)
select
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_owner,
    t.table_type,
    t.is_transient,
    t.clustering_key,
    t.row_count,
    t.bytes,
    t.retention_time,
    t.self_referencing_column_name,
    t.reference_generation,
    t.user_defined_type_catalog,
    t.user_defined_type_schema,
    t.user_defined_type_name,
    t.is_insertable_into,
    t.is_typed,
    t.created,
    t.last_altered,
    t.auto_clustering_on,
    t.comment as table_comment,
    v.comment as view_comment,
    v.view_definition,
    v.is_secure,
    v.is_updatable,
    array_to_string(u.nodes, ',') as upstream,
    array_to_string(d.nodes, ',') as downstream
from information_schema.tables t
left join information_schema.views as v
    on v.table_catalog = t.table_catalog
    and v.table_schema = t.table_schema
    and v.table_name = t.table_name
left join upstream u
    on u.node_database = t.table_catalog
    and u.node_schema = t.table_schema
    and u.node_name = t.table_name
left join downstream d
    on d.node_database = t.table_catalog
    and d.node_schema = t.table_schema
    and d.node_name = t.table_name
where t.table_schema != 'INFORMATION_SCHEMA'
order by table_catalog, table_schema, table_name

"""

COLUMNS_QUERY = """
select
   c.table_catalog,
   c.table_schema,
   c.table_name,
   c.column_name,
   c.ordinal_position,
   c.column_default,
   c.is_nullable,
   c.data_type,
   c.character_maximum_length,
   c.character_octet_length,
   c.numeric_precision,
   c.numeric_precision_radix,
   c.numeric_scale,
   c.collation_name,
   c.is_identity,
   c.identity_generation,
   c.identity_start,
   c.identity_increment,
   c.identity_cycle,
   c.comment
from information_schema.columns as c
join information_schema.tables as t
    on c.table_catalog = t.table_catalog
    and c.table_schema = t.table_schema
    and c.table_name = t.table_name
where c.table_schema != 'INFORMATION_SCHEMA'
order by
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.ordinal_position
"""

RAW_PIPES_QUERY = """

SELECT PIPE_CATALOG, PIPE_SCHEMA, PIPE_NAME, DEFINITION
FROM INFORMATION_SCHEMA.PIPES
;

"""

RAW_STAGES_QUERY = """

SELECT STAGE_NAME, STAGE_CATALOG, STAGE_SCHEMA, STAGE_URL, STAGE_TYPE
FROM INFORMATION_SCHEMA.STAGES
;

"""


class SnowflakeClientBase(ABC):
    def __init__(self, config: SnowflakePlugin):
        self._config = config

    @abstractmethod
    def get_tables(self) -> List[Table]:
        raise NotImplementedError

    @abstractmethod
    def get_raw_pipes(self) -> List[RawPipe]:
        raise NotImplementedError

    @abstractmethod
    def get_raw_stages(self) -> List[RawStage]:
        raise NotImplementedError


class SnowflakeClient(SnowflakeClientBase):
    @contextlib.contextmanager
    def connect(self):
        cursor = None
        connection = None

        try:
            connection = connector.connect(
                user=self._config.user,
                password=self._config.password.get_secret_value(),
                account=self._get_account(self._config.host),
                database=self._config.database,
                warehouse=self._config.warehouse,
            )
            cursor = DictCursor(connection)
            yield cursor
        except (DataError, ProgrammingError) as e:
            raise DataSourceError(
                "Error during getting information from Snowflake"
            ) from e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_tables(self) -> List[Union[Table, View]]:
        def is_belongs(table: Table) -> Callable[[Column], bool]:
            def _(column: Column) -> bool:
                return (
                    table.table_catalog == column.table_catalog
                    and table.table_schema == column.table_schema
                    and table.table_name == column.table_name
                )

            return _

        with self.connect() as cursor:
            tables: List[Table] = self._fetch_tables(cursor)
            columns: List[Column] = self._fetch_columns(cursor)

            for table in tables:
                belongs, not_belongs = lsplit(is_belongs(table), columns)
                table.columns.extend(belongs)
                columns = not_belongs

            return tables

    def get_raw_pipes(self) -> List[RawPipe]:
        with self.connect() as cursor:
            return self._fetch_something(RAW_PIPES_QUERY, cursor, RawPipe)

    def get_raw_stages(self) -> List[RawStage]:
        with self.connect() as cursor:
            return self._fetch_something(RAW_STAGES_QUERY, cursor, RawStage)

    def _fetch_tables(self, cursor: DictCursor) -> List[Table]:
        result: List[Table] = []

        cursor.execute(TABLES_VIEWS_QUERY)

        for raw_object in cursor.fetchall():
            if raw_object.get("TABLE_TYPE") == "BASE TABLE":
                result.append(Table.parse_obj(LowerKeyDict(raw_object)))
            elif raw_object.get("TABLE_TYPE") == "VIEW":
                result.append(View.parse_obj(LowerKeyDict(raw_object)))

        return result

    @staticmethod
    def _fetch_something(
        query: str,
        cursor: DictCursor,
        entity_type: Type[Union[Pipe, RawPipe, RawStage]],
    ) -> List[Union[Pipe, RawPipe, RawStage]]:
        result: List[entity_type] = []
        cursor.execute(query)
        for raw_object in cursor.fetchall():
            result.append(entity_type.parse_obj(LowerKeyDict(raw_object)))
        return result

    def _fetch_columns(self, cursor: DictCursor) -> List[Column]:
        cursor.execute(COLUMNS_QUERY)
        return [
            Column.parse_obj(LowerKeyDict(raw_column))
            for raw_column in cursor.fetchall()
        ]

    def _get_account(self, host: str) -> str:
        return host.split(".snowflakecomputing.com")[0]
