from dataclasses import asdict, dataclass

import mysql.connector
from odd_collector_sdk.errors import DataSourceConnectionError
from odd_collector_sdk.logger import logger

from odd_collector.domain.plugin import MySQLPlugin
from odd_collector.helpers.bytes_to_str import convert_bytes_to_str
from odd_collector.helpers.datetime import Datetime
from odd_collector.helpers.lower_key_dict import LowerKeyDict
from odd_collector.models import Column, Table


@dataclass(frozen=True)
class ConnectionParams:
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_disabled: bool

    @classmethod
    def from_config(cls, config: MySQLPlugin):
        return cls(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password.get_secret_value(),
            ssl_disabled=config.ssl_disabled,
        )


class Repository:
    def __init__(self, conn_params: ConnectionParams):
        """
        :param conn_params: MySql Connection parameters
        """
        self.conn_params = conn_params

    def __enter__(self):
        logger.debug("Connecting to MySQL")
        try:
            self.conn = mysql.connector.connect(**asdict(self.conn_params))
            return self
        except mysql.connector.Error as err:
            raise DataSourceConnectionError(
                f"Error connecting to MySQL: {err}"
            ) from err

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Closing connection to MySQL")
        self.conn.close()

    def get_tables(self, database: str) -> list[Table]:
        columns: list[Column] = self.get_columns()

        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(self.tables_query, (database,))

            tables = []
            for raw in cursor.fetchall():
                raw = {**LowerKeyDict(raw)}
                table = Table(
                    catalog=raw.pop("table_catalog"),
                    schema=raw.pop("table_schema"),
                    comment=raw.pop("table_comment"),
                    create_time=Datetime(raw.pop("create_time")),
                    update_time=Datetime(raw.pop("update_time")),
                    name=(table_name := raw.pop("table_name")),
                    type=raw.pop("table_type"),
                    sql_definition=raw.pop("view_definition"),
                    table_rows=raw.pop("table_rows"),
                    metadata=raw,
                    columns=[
                        column for column in columns if column.table_name == table_name
                    ],
                )
                tables.append(table)

            return tables

    def get_columns(self) -> list[Column]:
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(self.columns_query)
            columns = []

            for raw in cursor.fetchall():
                raw = {**LowerKeyDict(raw)}
                column = Column(
                    table_catalog=raw.pop("table_catalog"),
                    table_name=raw.pop("table_name"),
                    table_schema=raw.pop("table_schema"),
                    name=raw.pop("column_name"),
                    type=convert_bytes_to_str(raw.pop("data_type")),
                    is_nullable=raw.pop("is_nullable"),
                    comment=convert_bytes_to_str(raw.pop("column_comment")),
                    default=raw.pop("column_default"),
                    metadata=raw,
                )
                columns.append(column)

            return columns

    @property
    def tables_query(self):
        return f"""
        select t.table_catalog,
               t.table_schema,
               t.table_name,
               t.table_type,
               t.engine,
               t.version,
               t.row_format,
               t.table_rows,
               t.avg_row_length,
               t.data_length,
               t.max_data_length,
               t.index_length,
               t.data_free,
               t.auto_increment,
               t.create_time,
               t.update_time,
               t.check_time,
               t.table_collation,
               t.checksum,
               t.create_options,
               t.table_comment,
               v.view_definition
        from information_schema.tables t
                 left join information_schema.views v
                           on t.TABLE_CATALOG = v.TABLE_CATALOG and
                              t.TABLE_SCHEMA = v.TABLE_SCHEMA and
                              t.TABLE_NAME = v.TABLE_NAME
        where t.table_schema = %s
        order by t.table_catalog, t.table_schema, t.table_name
        """

    @property
    def columns_query(self):
        return """
            select
                table_catalog,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                column_default,
                is_nullable,
                data_type,
                character_maximum_length,
                character_octet_length,
                numeric_precision,
                numeric_scale,
                datetime_precision,
                character_set_name,
                collation_name,
                column_type,
                column_key,
                extra,
                privileges,
                column_comment,
                generation_expression
            from information_schema.columns
            where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
            order by table_catalog, table_schema, table_name, ordinal_position;
        """
