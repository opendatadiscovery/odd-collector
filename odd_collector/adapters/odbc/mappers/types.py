# https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
# Exact numerics:
# tinyint, smallint, int, bigint, numeric, decimal, smallmoney, money, bit
# Approximate numerics:
# float, real
# Date and time:
# smalldatetime, datetime, datetime2, datetimeoffset, date, time
# Character strings:
# char, varchar, text
# Unicode character strings:
# nchar, nvarchar, ntext
# Binary strings:
# binary, varbinary, image
# Other data types:
# uniqueidentifier, sql_variant, xml, rowversion, hierarchyid, cursor, table,
# Spatial Geometry Types, Spatial Geography Types
from odd_models.models import Type, DataEntityType

TYPES_SQL_TO_ODD: dict[str, Type] = {
    "tinyint": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,
    "numeric": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "smallmoney": Type.TYPE_NUMBER,
    "money": Type.TYPE_NUMBER,
    "bit": Type.TYPE_BINARY,
    "float": Type.TYPE_NUMBER,
    "real": Type.TYPE_NUMBER,
    "smalldatetime": Type.TYPE_DATETIME,
    "datetime": Type.TYPE_DATETIME,
    "datetime2": Type.TYPE_DATETIME,
    "datetimeoffset": Type.TYPE_DATETIME,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_DATETIME,
    "char": Type.TYPE_CHAR,
    "varchar": Type.TYPE_STRING,
    "text": Type.TYPE_STRING,
    "nchar": Type.TYPE_CHAR,
    "nvarchar": Type.TYPE_STRING,
    "ntext": Type.TYPE_STRING,
    "binary": Type.TYPE_BINARY,
    "varbinary": Type.TYPE_BINARY,
    "image": Type.TYPE_BINARY,
    "uniqueidentifier": Type.TYPE_STRING,
    "xml": Type.TYPE_STRING
    # Other data types
}

TABLE_TYPES_SQL_TO_ODD: dict[str, DataEntityType] = {
    "BASE TABLE": DataEntityType.TABLE,
    "TABLE": DataEntityType.TABLE,
    "VIEW": DataEntityType.VIEW,
}
