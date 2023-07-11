from typing import Dict

from odd_models.models import Type

# Excluded types: BLOB, TIMEUUID, UUID, USER-DEFINED TODO add those types

TYPES_CASSANDRA_TO_ODD: Dict[str, Type] = {
    "uuid": Type.TYPE_STRING,
    "ascii": Type.TYPE_STRING,
    "text": Type.TYPE_STRING,
    "varchar": Type.TYPE_STRING,
    "inet": Type.TYPE_STRING,
    "duration": Type.TYPE_DURATION,
    "boolean": Type.TYPE_BOOLEAN,
    "counter": Type.TYPE_INTEGER,
    "tinyint": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,
    "varint": Type.TYPE_INTEGER,
    "decimal": Type.TYPE_NUMBER,
    "float": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "time": Type.TYPE_TIME,
    "date": Type.TYPE_DATETIME,
    "timestamp": Type.TYPE_DATETIME,
    "map": Type.TYPE_MAP,
    "list": Type.TYPE_LIST,
    "set": Type.TYPE_LIST,
    "tuple": Type.TYPE_LIST,
}
