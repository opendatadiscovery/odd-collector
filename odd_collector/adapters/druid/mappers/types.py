from typing import Dict

from odd_models.models import Type

TYPES_DRUID_TO_ODD: Dict[str, Type] = {
    "VARCHAR": Type.TYPE_STRING,
    "DOUBLE": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "LONG": Type.TYPE_NUMBER,
    "COMPLEX": Type.TYPE_UNKNOWN
}
