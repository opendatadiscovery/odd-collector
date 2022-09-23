from typing import Dict

from odd_models.models import Type

TYPES_DRUID_TO_ODD: Dict[str, Type] = {
    "CHAR": Type.TYPE_STRING,
    "VARCHAR": Type.TYPE_STRING,
    "DECIMAL": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "REAL": Type.TYPE_NUMBER,
    "DOUBLE": Type.TYPE_NUMBER,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "TINYINT": Type.TYPE_NUMBER,
    "SMALLINT": Type.TYPE_NUMBER,
    "INTEGER": Type.TYPE_NUMBER,
    "BIGINT": Type.TYPE_NUMBER,
    "TIMESTAMP": Type.TYPE_NUMBER,
    "DATE": Type.TYPE_DATETIME,
    "OTHER": Type.TYPE_UNKNOWN,
}
