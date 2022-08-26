# See for development:
# List of SQL Data Types in vertica:
#   https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/SQLDataTypes.htm#0
# System table TYPES which Provides information about supported data types:
#   https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/SQLReferenceManual/SystemTables/CATALOG/TYPES.htm

from odd_models.models import Type, DataEntityType
from typing import Dict


TYPES_SQL_TO_ODD: Dict[str, Type] = {
    "binary": Type.TYPE_BINARY,
    "varbinary": Type.TYPE_BINARY,
    "long varbinary": Type.TYPE_BINARY,
    "bytea": Type.TYPE_BINARY,
    "raw": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "char": Type.TYPE_STRING,
    "varchar": Type.TYPE_STRING,
    "long varchar": Type.TYPE_STRING,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_DATETIME,
    "datetime": Type.TYPE_DATETIME,
    "smalldatetime": Type.TYPE_DATETIME,
    "time with timezone": Type.TYPE_DATETIME,
    "timestamp": Type.TYPE_DATETIME,
    "timestamp with timezone": Type.TYPE_DATETIME,
    "interval": Type.TYPE_DATETIME,
    "interval day to second": Type.TYPE_DATETIME,
    "interval year to month": Type.TYPE_DATETIME,
    "double precision": Type.TYPE_NUMBER,
    "float": Type.TYPE_NUMBER,
    "float8": Type.TYPE_NUMBER,
    "real": Type.TYPE_NUMBER,
    "integer": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,
    "int8": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "tinyint": Type.TYPE_INTEGER,
    "decimal": Type.TYPE_INTEGER,
    "numeric": Type.TYPE_INTEGER,
    "number": Type.TYPE_INTEGER,
    "money": Type.TYPE_INTEGER,
    "geometry": Type.TYPE_UNKNOWN,
    "geography": Type.TYPE_UNKNOWN,
    "uuid": Type.TYPE_STRING,
}

TABLE_TYPES_SQL_TO_ODD: Dict[str, DataEntityType] = {
    "TABLE": DataEntityType.TABLE,
    "VIEW": DataEntityType.VIEW,
}
