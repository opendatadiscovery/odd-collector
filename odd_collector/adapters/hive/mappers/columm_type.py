from odd_models.models import Type

from odd_collector.adapters.hive.models.column_types import ColumnType

TYPES_HIVE_TO_ODD = {
    "int": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "tinyint": Type.TYPE_INTEGER,
    "double": Type.TYPE_NUMBER,
    "double precision": Type.TYPE_NUMBER,
    "bigint": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "float": Type.TYPE_NUMBER,
    "string": Type.TYPE_STRING,
    "varchar": Type.TYPE_STRING,
    "boolean": Type.TYPE_BOOLEAN,
    "char": Type.TYPE_CHAR,
    "date": Type.TYPE_DATETIME,
    "timestamp": Type.TYPE_DATETIME,
    "binary": Type.TYPE_BINARY,
    "array": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "struct": Type.TYPE_STRUCT,
    "union": Type.TYPE_UNION,
}


def map_column_type(column_type: ColumnType):
    odd_type = TYPES_HIVE_TO_ODD.get(column_type.field_type)

    if not odd_type:
        odd_type = Type.TYPE_UNKNOWN

    return odd_type
