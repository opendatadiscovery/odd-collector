# https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-datatypes

from typing import Dict

from odd_models.models import Type

TYPES_SQL_TO_ODD: Dict[str, Type] = {
    "float": Type.TYPE_NUMBER,
    "struct": Type.TYPE_STRUCT,
    "bigint": Type.TYPE_INTEGER,
    "binary": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "date": Type.TYPE_DATETIME,
    "decimal": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "void": Type.TYPE_UNKNOWN,
    "smallint": Type.TYPE_INTEGER,
    "timestamp": Type.TYPE_TIME,
    "tinyint": Type.TYPE_INTEGER,
    "array": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "int": Type.TYPE_INTEGER,
    "string": Type.TYPE_STRING,
    "interval": Type.TYPE_DURATION,
}
