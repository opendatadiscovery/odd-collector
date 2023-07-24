from pathlib import Path
from typing import Iterable

from duckdb import DuckDBPyConnection, connect

from odd_collector.adapters.duckdb.mappers.models import DuckDBColumn, DuckDBTable


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
    def get_schemas(connection: DuckDBPyConnection, catalog: str) -> list[str]:
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
    ) -> list[DuckDBTable]:
        metadata = connection.execute(
            query=f"""
            SELECT table_catalog, table_schema, table_name, table_type, is_insertable_into, is_typed 
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ?
            """,
            parameters=(catalog, schema),
        ).fetchall()

        tables = [
            DuckDBTable(
                catalog=item[0],
                schema=item[1],
                name=item[2],
                type=item[3],
                odd_metadata={
                    "table_type": item[3],
                    "is_insertable_into": item[4],
                    "is_typed": item[5],
                },
            )
            for item in metadata
        ]
        return tables

    @staticmethod
    def get_columns_metadata(
        connection: DuckDBPyConnection, catalog: str, schema: str, table: str
    ) -> list[DuckDBColumn]:
        metadata = connection.execute(
            query=f"""
            SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length, numeric_precision 
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            """,
            parameters=(catalog, schema, table),
        ).fetchall()

        columns = [
            DuckDBColumn(
                table_catalog=item[0],
                table_schema=item[1],
                table_name=item[2],
                name=item[3],
                is_nullable=item[4],
                type=item[5],
                odd_metadata={
                    "is_nullable": item[4],
                    "data_type": item[5],
                    "character_maximum_length": item[6],
                    "numeric_precision": item[7],
                },
            )
            for item in metadata
        ]
        return columns
