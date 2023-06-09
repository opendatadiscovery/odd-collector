# https://duckdb.org/docs/sql/data_types/overview

from odd_models.models import Type
TYPES_SQL_TO_ODD: dict[str, Type] = {
    "BIGINT": Type.TYPE_INTEGER,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "BLOB": Type.TYPE_BINARY,
    "DATE": Type.TYPE_DATETIME,
    "DOUBLE": Type.TYPE_NUMBER,
    "DECIMAL": Type.TYPE_NUMBER,
    "HUGEINT": Type.TYPE_INTEGER,
    "INTEGER": Type.TYPE_INTEGER,
    "REAL": Type.TYPE_NUMBER,
    "SMALLINT": Type.TYPE_INTEGER,
    "TIME": Type.TYPE_TIME,
    "TIMESTAMP": Type.TYPE_TIME,
    "TINYINT": Type.TYPE_INTEGER,
    "UBIGINT": Type.TYPE_INTEGER,
    "UINTEGER": Type.TYPE_INTEGER,
    "USMALLINT": Type.TYPE_INTEGER,
    "UTINYINT": Type.TYPE_INTEGER,
    "UUID": Type.TYPE_STRING,
    "VARCHAR": Type.TYPE_STRING,
    "INTERVAL": Type.TYPE_DURATION,
    "ARRAY": Type.TYPE_LIST,
    "MAP": Type.TYPE_MAP,
    "STRUCT": Type.TYPE_STRUCT,
}

