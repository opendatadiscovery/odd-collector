# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# 1) integer, smallint, decimal, numeric, float, real, double precision, bit
# date, time, datetime, timestamp, year
# char, varchar, binary, varbinary, blob, text, enum, set, json
# tinyblob, tinytext, mediumblob, mediumtext, longblob, longtext
# 2) tinyint, smallint, mediumint, int, integer, bigint
# float, double, double precision, real, decimal, numeric, bit
from odd_models.models import Type, DataEntityType
from typing import Dict

TYPES_SQL_TO_ODD: Dict[str, Type] = {
    "tinyint": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "mediumint": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "integer": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "real": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "double precision": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "numeric": Type.TYPE_NUMBER,
    "bit": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "char": Type.TYPE_CHAR,
    "varchar": Type.TYPE_STRING,
    "tinytext": Type.TYPE_STRING,
    "mediumtext": Type.TYPE_STRING,
    "longtext": Type.TYPE_STRING,
    "text": Type.TYPE_STRING,
    "interval": Type.TYPE_DURATION,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_DATETIME,
    "datetime": Type.TYPE_DATETIME,
    "timestamp": Type.TYPE_DATETIME,
    "year": Type.TYPE_INTEGER,
    "binary": Type.TYPE_BINARY,
    "varbinary": Type.TYPE_BINARY,
    "tinyblob": Type.TYPE_BINARY,
    "mediumblob": Type.TYPE_BINARY,
    "longblob": Type.TYPE_BINARY,
    "blob": Type.TYPE_BINARY,
    "json": Type.TYPE_STRING,
    "enum": Type.TYPE_UNION,
    "set": Type.TYPE_LIST,
}

# base tables and views
TABLE_TYPES_SQL_TO_ODD: Dict[str, DataEntityType] = {
    "BASE TABLE": DataEntityType.TABLE,
    "VIEW": DataEntityType.VIEW,
}
