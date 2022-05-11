# See for development:
# https://www.tarantool.io/en/doc/latest/book/box/data_model/#data-types

from odd_models.models import Type

TYPES_Tarantool_TO_ODD: dict[str, Type] = {
    "nil": Type.TYPE_UNKNOWN,
    "boolean": Type.TYPE_BOOLEAN,
    "integer": Type.TYPE_INTEGER,
    "unsigned": Type.TYPE_INTEGER,
    "double": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "string": Type.TYPE_STRING,
    "bin": Type.TYPE_BINARY,
    "uuid": Type.TYPE_STRING,
    "array": Type.TYPE_LIST,
    "table": Type.TYPE_STRUCT,
    "scalar": Type.TYPE_UNKNOWN,
    "any": Type.TYPE_UNKNOWN,
}
