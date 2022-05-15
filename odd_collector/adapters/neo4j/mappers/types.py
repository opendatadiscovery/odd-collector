# https://neo4j.com/docs/cypher-manual/current/syntax/values/#property-types
from odd_models.models import Type

TYPES_SQL_TO_ODD: dict[str, Type] = {

    "integer": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "boolean": Type.TYPE_BOOLEAN,
    "string": Type.TYPE_STRING,

    "duration": Type.TYPE_DURATION,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_DATETIME,
    "datetime": Type.TYPE_DATETIME,
    "localtime": Type.TYPE_DATETIME,
    "localdatetime": Type.TYPE_DATETIME
}
