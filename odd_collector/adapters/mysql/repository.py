from dataclasses import asdict, dataclass
from typing import Any

import mysql.connector
from odd_collector_sdk.errors import DataSourceError

from odd_collector.domain.plugin import MySQLPlugin
from odd_collector.helpers.bytes_to_str import convert_bytes_to_str
from odd_collector.helpers.datetime import Datetime
from odd_collector.models import Column, Table


@dataclass
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
    def __init__(self, config: MySQLPlugin):
        self.config = config

    def get_tables(self) -> list[Table]:
        columns: list[Column] = self.get_columns()
        tables = []

        for raw in self.__execute(self.tables_query):
            table = Table(
                catalog=raw.pop("TABLE_CATALOG"),
                schema=raw.pop("TABLE_SCHEMA"),
                comment=raw.pop("TABLE_COMMENT"),
                create_time=Datetime(raw.pop("CREATE_TIME")),
                update_time=Datetime(raw.pop("UPDATE_TIME")),
                name=(table_name := raw.pop("TABLE_NAME")),
                type=raw.pop("TABLE_TYPE"),
                sql_definition=raw.pop("VIEW_DEFINITION"),
                table_rows=raw.pop("TABLE_ROWS"),
                metadata=raw,
                columns=[
                    column for column in columns if column.table_name == table_name
                ],
            )
            tables.append(table)

        return tables

    def get_columns(self) -> list[Column]:
        columns = []

        for raw in self.__execute(self.columns_query):
            column = Column(
                table_catalog=raw.pop("TABLE_CATALOG"),
                table_name=raw.pop("TABLE_NAME"),
                table_schema=raw.pop("TABLE_SCHEMA"),
                name=raw.pop("COLUMN_NAME"),
                type=convert_bytes_to_str(raw.pop("DATA_TYPE")),
                is_nullable=raw.pop("IS_NULLABLE"),
                comment=convert_bytes_to_str(raw.pop("COLUMN_COMMENT")),
                default=raw.pop("COLUMN_DEFAULT"),
                metadata=raw,
            )
            columns.append(column)

        return columns

    def __execute(self, query: str) -> list[dict[str, Any]]:
        assert query is not None

        try:
            with mysql.connector.connect(
                **asdict(ConnectionParams.from_config(config=self.config))
            ) as mysql_conn:
                with mysql_conn.cursor(dictionary=True) as mysql_cur:
                    mysql_cur.execute(query)
                    return mysql_cur.fetchall()
        except mysql.connector.Error as err:
            raise DataSourceError(
                f"Failed to connect to MySQL database, {err}"
            ) from err

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
        where t.table_schema = '{self.config.database}'
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
