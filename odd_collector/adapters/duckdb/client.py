import duckdb


class DuckDBClient:
    def __init__(self, connection: str):
        self.conn = duckdb.connect(connection)

    def get_schemas(self, catalog: str):
        resp = self.conn.sql(
            f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{catalog}'"
        ).fetchall()
        schemas = [
            item[0]
            for item in resp
            if item[0] not in ["pg_catalog", "information_schema"]
        ]
        return schemas

    def get_tables_metadata(self, catalog: str, schema: str) -> list[dict]:
        tables = self.conn.sql(
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

    def get_columns_metadata(self, catalog: str, schema: str, table: str) -> list[dict]:
        columns = self.conn.sql(
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
