from typing import Dict

from odd_models.models import Type


TYPES_FIVETRAN_TO_ODD: Dict[str, Type] = {
    "boolean": Type.TYPE_BOOLEAN,
    "short": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "long": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "bigdecimal": Type.TYPE_NUMBER,
    "localdata": Type.TYPE_DATETIME,
    "instant": Type.TYPE_DATETIME,
    "localdatetime": Type.TYPE_DATETIME,
    "string": Type.TYPE_STRING,
    "xml": Type.TYPE_STRING,
    "json": Type.TYPE_STRING,
    "binary": Type.TYPE_BINARY,
}
