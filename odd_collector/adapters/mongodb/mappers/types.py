# source https://pymongo.readthedocs.io/en/stable/api/bson/index.html

"""
BSON (Binary JSON) encoding and decoding.

The mapping from Python types to BSON types is as follows:

=======================================  =============  ===================
Python Type                              BSON Type      Supported Direction
=======================================  =============  ===================
None                                     null           both
bool                                     boolean        both
int [#int]_                              int32 / int64  py -> bson
`bson.int64.Int64`                       int64          both
float                                    number (real)  both
str                                      string         both
list                                     array          both
dict / `SON`                             object         both
datetime.datetime [#dt]_ [#dt2]_         date           both
`bson.regex.Regex`                       regex          both
compiled re [#re]_                       regex          py -> bson
`bson.binary.Binary`                     binary         both
`bson.objectid.ObjectId`                 oid            both
`bson.dbref.DBRef`                       dbref          both
None                                     undefined      bson -> py
`bson.code.Code`                         code           both
str                                      symbol         bson -> py
bytes [#bytes]_                          binary         both
=======================================  =============  ===================
"""

from odd_models.models import Type, DataEntityType
from typing import Dict

TYPES_MONGO_TO_ODD: Dict[str, Type] = {
    "date": Type.TYPE_DATETIME,
    "text": Type.TYPE_STRING,
    "json": Type.TYPE_STRING,
    "jsonb": Type.TYPE_STRING,
    "tsvector": Type.TYPE_STRING,
    "bit": Type.TYPE_BINARY,  # BITOID recheck
    "Binary": Type.TYPE_BINARY,
    "bytes": Type.TYPE_BINARY,
    "bool": Type.TYPE_BOOLEAN,  # BOOLOID
    "char": Type.TYPE_CHAR,  # BPCHAROID recheck
    "real": Type.TYPE_NUMBER,  # FLOAT4OID
    "double precision": Type.TYPE_NUMBER,  # FLOAT8OID
    "float": Type.TYPE_NUMBER,
    "smallint": Type.TYPE_INTEGER,  # INT2OID
    "int": Type.TYPE_INTEGER,  # INT4OID
    "Int64": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,  # INT8OID recheck
    "numeric": Type.TYPE_NUMBER,  # NUMERICOID
    "interval": Type.TYPE_DURATION,  # INTERVALOID recheck
    "time": Type.TYPE_TIME,  # TIMEOID, TIMETZOID
    "time without time zone": Type.TYPE_TIME,  # TIMEOID
    "time with time zone": Type.TYPE_TIME,  # TIMETZOID
    "datetime": Type.TYPE_DATETIME,  # TIMESTAMPOID, TIMESTAMPTZOID
    "timestamp without time zone": Type.TYPE_DATETIME,  # TIMESTAMPOID
    "timestamp with time zone": Type.TYPE_DATETIME,  # TIMESTAMPTZOID
    "bit varying": Type.TYPE_BINARY,  # VARBITOID recheck
    "str": Type.TYPE_STRING,  # VARCHAROID
    "ObjectId": Type.TYPE_STRING,  # VARCHAROID
    "dict": Type.TYPE_STRUCT,
    "Object": Type.TYPE_STRUCT,
    "list": Type.TYPE_LIST,  # view information_schema.columns recheck
    "USER-DEFINED": Type.TYPE_STRUCT,  # view information_schema.columns recheck
    "Regex": Type.TYPE_UNKNOWN,
    "DBRef": Type.TYPE_UNKNOWN,
    "None": Type.TYPE_UNKNOWN,
    "Code": Type.TYPE_UNKNOWN,
}
