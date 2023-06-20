from pathlib import Path
from typing import Iterable
from duckdb import connect, DuckDBPyConnection


class NotValidPathError(Exception):
    def __init__(self):
        message = "None of the provided paths contains a valid database file"
        super().__init__(message)


class DuckDBClient:
    def __init__(self, paths: list[Path]):
        self.db_files = self.__get_db_files(paths)

    @staticmethod
    def __validate_paths(paths: list[Path]) -> Iterable[Path]:
        for path in paths:
            if path.is_file():
                yield path
            elif path.is_dir():
                files_paths = path.rglob("*.*")
                yield from files_paths

    def __get_db_files(self, paths: list[Path]) -> dict[str, Path]:
        validated_paths = list(self.__validate_paths(paths))
        if validated_paths:
            return {path.stem: path for path in validated_paths}
        else:
            raise NotValidPathError

    def get_connection(self, catalog: str) -> DuckDBPyConnection:
        return connect(str(self.db_files[catalog]))

    @staticmethod
    def get_schemas(connection: DuckDBPyConnection, catalog: str):
        resp = connection.execute(
            query=f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?",
            parameters=(catalog,),
        ).fetchall()
        schemas = [
            item[0]
            for item in resp
            if item[0] not in ["pg_catalog", "information_schema"]
        ]
        return schemas

    @staticmethod
    def get_tables_metadata(
        connection: DuckDBPyConnection, catalog: str, schema: str
    ) -> list[dict]:
        tables = connection.execute(
            query=f"""
            SELECT table_catalog, table_schema, table_name, table_type, is_insertable_into, is_typed 
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ?
            """,
            parameters=(catalog, schema),
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
    def get_columns_metadata(
        connection: DuckDBPyConnection, catalog: str, schema: str, table: str
    ) -> list[dict]:
        columns = connection.execute(
            query=f"""
            SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, numeric_precision 
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            """,
            parameters=(catalog, schema, table),
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
