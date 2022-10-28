from abc import ABC, abstractmethod
from typing import List

from odd_collector.adapters.snowflake.connectors import SnowflakeConnector


class AbstractRepository(ABC):
    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_columns(self):
        pass


class SnowflakeRepository(AbstractRepository):
    def __init__(self, config):
        self.__snowflake_connector = SnowflakeConnector(config)

    def get_tables(self) -> List[tuple]:
        return self.__execute(self.table_metadata_query)

    def get_columns(self) -> List[tuple]:
        return self.__execute(self.column_metadata_query)

    def __execute(self, query: str) -> List[tuple]:
        with self.__snowflake_connector.connection() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    @property
    def table_metadata_query(self):
        return """
             SELECT
            t.TABLE_CATALOG,
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_OWNER,
            t.TABLE_TYPE,
            t.ROW_COUNT,
            t.IS_TRANSIENT,
            t.CLUSTERING_KEY,
            t.BYTES,
            t.RETENTION_TIME,
            t.CREATED,
            t.LAST_ALTERED,
            t.AUTO_CLUSTERING_ON,
            t.COMMENT,
            v.VIEW_DEFINITION,
            v.IS_SECURE
        FROM
        INFORMATION_SCHEMA.TABLES as t
        LEFT JOIN INFORMATION_SCHEMA.VIEWS as v
        on(
        v.TABLE_CATALOG = t.TABLE_CATALOG and v.TABLE_SCHEMA = t.TABLE_SCHEMA and v.TABLE_NAME = t.TABLE_NAME
    )
    WHERE
    t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
    ORDER
    BY
    t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME;
"""

    @property
    def column_metadata_query(self):
        return """
            SELECT
                c.TABLE_CATALOG,
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.ORDINAL_POSITION,
                c.COLUMN_DEFAULT,
                c.IS_NULLABLE,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.CHARACTER_OCTET_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_PRECISION_RADIX,
                c.NUMERIC_SCALE,
                c.COLLATION_NAME,
                c.IS_IDENTITY,
                c.IDENTITY_GENERATION,
                c.IDENTITY_START,
                c.IDENTITY_INCREMENT,
                c.IDENTITY_CYCLE,
                c.COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS as c
                JOIN INFORMATION_SCHEMA.TABLES as t on (
                c.TABLE_CATALOG = t.TABLE_CATALOG and c.TABLE_SCHEMA = t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME
)
            WHERE c.TABLE_SCHEMA != 'INFORMATION_SCHEMA';
        """









