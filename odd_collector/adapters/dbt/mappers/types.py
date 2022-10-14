from odd_models.models import DataEntityType, Type

TYPES_SNOWFLAKE_TO_ODD: dict[str, Type] = {
    "INT": Type.TYPE_INTEGER,
    "INTEGER": Type.TYPE_INTEGER,
    "SMALLINT": Type.TYPE_INTEGER,
    "BIGINT": Type.TYPE_INTEGER,
    "NUMBER": Type.TYPE_NUMBER,
    "DECIMAL": Type.TYPE_NUMBER,
    "NUMERIC": Type.TYPE_NUMBER,
    "DOUBLE": Type.TYPE_NUMBER,
    "REAL": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "FIXED": Type.TYPE_NUMBER,
    "STRING": Type.TYPE_STRING,
    "TEXT": Type.TYPE_STRING,
    "VARCHAR": Type.TYPE_STRING,
    "CHAR": Type.TYPE_CHAR,
    "CHARACTER": Type.TYPE_CHAR,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "DATETIME": Type.TYPE_DATETIME,
    "DATE": Type.TYPE_DATETIME,
    "TIMESTAMP": Type.TYPE_DATETIME,
    "TIMESTAMP_LTZ": Type.TYPE_DATETIME,
    "TIMESTAMP_NTZ": Type.TYPE_DATETIME,
    "TIMESTAMP_TZ": Type.TYPE_DATETIME,
    "ARRAY": Type.TYPE_LIST,
    "VARIANT": Type.TYPE_LIST,
}

# base tables and views
TABLE_TYPES_SQL_TO_ODD = {
    "TABLE": DataEntityType.TABLE,
    "BASE TABLE": DataEntityType.TABLE,
    "VIEW": DataEntityType.VIEW,
}
