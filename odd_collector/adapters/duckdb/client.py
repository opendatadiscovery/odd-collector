import os
import re
from duckdb import connect, DuckDBPyConnection


class DuckDBClient:
    def __init__(self, paths: list[str]):
        self.db_files = self.__get_db_files(paths)

    def __validate_paths(self, paths: list[str]):
        for path in paths:
            if os.path.isfile(path):
                yield path
            elif os.path.isdir(path):
                new_paths = [os.path.join(path, file) for file in os.listdir(path)]
                yield from self.__validate_paths(new_paths)

    def __get_db_files(self, paths: list[str]):
        files = list(self.__validate_paths(paths))
        print(files)
        return {re.search(r"\/([^\/]*)\.", file).group(1): file for file in files}

    def get_connection(self, catalog: str) -> DuckDBPyConnection:
        return connect(self.db_files[catalog])

    @staticmethod
    def get_schemas(connection: DuckDBPyConnection, catalog: str):
        resp = connection.sql(
            f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{catalog}'"
        ).fetchall()
        schemas = [
            item[0]
            for item in resp
            if item[0] not in ["pg_catalog", "information_schema"]
        ]
        return schemas

    @staticmethod
    def get_tables_metadata(connection: DuckDBPyConnection, catalog: str, schema: str) -> list[dict]:
        tables = connection.sql(
            f"""
            SELECT table_catalog, table_schema, table_name, table_type, is_insertable_into, is_typed 
            FROM information_schema.tables
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}'
            """
        ).fetchall()
        metadata = [
            {
                "table_catalog": table[0],
                "table_schema": table[1],
                "table_name": table[2],
                "table_type": table[3],
                "is_insertable_into": table[4],
                "is_typed": table[5],
            }
            for table in tables
        ]
        return metadata

    @staticmethod
    def get_columns_metadata(connection: DuckDBPyConnection, catalog: str, schema: str, table: str) -> list[dict]:
        columns = connection.sql(
            f"""
            SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, numeric_precision 
            FROM information_schema.columns
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}'
            """
        ).fetchall()
        metadata = [
            {
                "table_catalog": column[0],
                "table_schema": column[1],
                "table_name": column[2],
                "column_name": column[3],
                "is_nullable": column[4],
                "data_type": column[5],
                "character_maximum_length": column[6],
                "numeric_precision": column[7],
            }
            for column in columns
        ]
        return metadata
