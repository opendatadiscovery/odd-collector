from typing import Dict

from odd_models.models import Type

TYPES_COUCHBASE_TO_ODD: Dict[str, Type] = {
    "object": Type.TYPE_STRUCT,
    "array": Type.TYPE_LIST,
    "string": Type.TYPE_STRING,
    "number": Type.TYPE_NUMBER,
    "null": Type.TYPE_UNKNOWN,
    "boolean": Type.TYPE_BOOLEAN,
    "union": Type.TYPE_UNION,
    "int": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
}
