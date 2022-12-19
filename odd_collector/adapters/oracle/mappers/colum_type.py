from odd_models.models import Type

from odd_collector.adapters.oracle.domain import ColumnType


def map_type(colum_type: ColumnType) -> Type:
    mapping = {
        ColumnType.STRING: Type.TYPE_STRING,
        ColumnType.NUMBER: Type.TYPE_NUMBER,
        ColumnType.INTEGER: Type.TYPE_INTEGER,
        ColumnType.BOOLEAN: Type.TYPE_BOOLEAN,
        ColumnType.CHAR: Type.TYPE_CHAR,
        ColumnType.DATETIME: Type.TYPE_DATETIME,
        ColumnType.TIME: Type.TYPE_TIME,
        ColumnType.BINARY: Type.TYPE_BINARY,
        ColumnType.UNKNOWN: Type.TYPE_UNKNOWN,
    }

    return mapping.get(colum_type, Type.TYPE_UNKNOWN)
