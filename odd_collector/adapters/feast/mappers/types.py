from feast.value_type import ValueType
from odd_models.models import Type

TYPES_FEAST_FEATURE_TO_ODD: dict[int, dict] = {
    0: {"field_type": Type.TYPE_UNKNOWN},  # UNKNOWN
    1: {"field_type": Type.TYPE_BINARY},  # BYTES
    2: {"field_type": Type.TYPE_STRING},  # STRING
    3: {"field_type": Type.TYPE_INTEGER},  # INT32
    4: {"field_type": Type.TYPE_INTEGER},  # INT64
    5: {"field_type": Type.TYPE_NUMBER},  # DOUBLE
    6: {"field_type": Type.TYPE_NUMBER},  # FLOAT
    7: {"field_type": Type.TYPE_BOOLEAN},  # BOOL
    8: {"field_type": Type.TYPE_DATETIME},  # UNIX_TIMESTAMP
    11: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.BYTES),
    },  # BYTES_LIST
    12: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.STRING),
    },  # STRING_LIST
    13: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.INT32),
    },  # INT32_LIST
    14: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.INT64),
    },  # INT64_LIST
    15: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.DOUBLE),
    },  # DOUBLE_LIST
    16: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.FLOAT),
    },  # FLOAT_LIST
    17: {"field_type": Type.TYPE_LIST, "child": ValueType(ValueType.BOOL)},  # BOOL_LIST
    18: {
        "field_type": Type.TYPE_LIST,
        "child": ValueType(ValueType.UNIX_TIMESTAMP),
    },  # UNIX_TIMESTAMP_LIST
}
