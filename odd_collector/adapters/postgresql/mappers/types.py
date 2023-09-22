# https://www.postgresql.org/docs/current/datatype.html
# The following types (or spellings thereof) are specified by SQL:
# bigint, bit, bit varying, boolean, char, character varying, character, varchar,
# date, double precision, integer, interval, numeric, decimal, real, smallint,
# time (with or without time zone), timestamp (with or without time zone), xml.
#
# See for development:
# view information_schema.columns, routine pg_catalog.format_type
# source https://github.com postgres/postgres src/backend/utils/adt/format_type.c
# https://github.com/postgres/postgres/blob/ca3b37487be333a1d241dab1bbdd17a211a88f43/src/backend/utils/adt/format_type.c
#
# Constant          Non Typmod                      Typmod              Exception
# BITOID            'bit'                           'bit'               +
# BOOLOID           'boolean'
# BPCHAROID         'character'                     'character'         +
# FLOAT4OID         'real'
# FLOAT8OID         'double precision'
# INT2OID           'smallint'
# INT4OID           'integer'
# INT8OID           'bigint'
# NUMERICOID        'numeric'                       'numeric'
# INTERVALOID       'interval'                      'interval'
# TIMEOID           'time without time zone'        'time'
# TIMETZOID         'time with time zone'           'time'
# TIMESTAMPOID      'timestamp without time zone'   'timestamp'
# TIMESTAMPTZOID    'timestamp with time zone'      'timestamp'
# VARBITOID         'bit varying'                   'bit varying'
# VARCHAROID        'character varying'             'character varying'

from typing import Dict

from odd_models.models import DataEntityType, Type

TYPES_SQL_TO_ODD: Dict[str, Type] = {
    "date": Type.TYPE_DATETIME,
    "text": Type.TYPE_STRING,
    "varchar": Type.TYPE_STRING,
    "_varchar": Type.TYPE_STRING,
    "json": Type.TYPE_STRING,
    "jsonb": Type.TYPE_STRING,
    "tsvector": Type.TYPE_STRING,
    "name": Type.TYPE_CHAR,
    "bit": Type.TYPE_BINARY,  # BITOID recheck
    "boolean": Type.TYPE_BOOLEAN,  # BOOLOID
    "character": Type.TYPE_CHAR,  # BPCHAROID recheck
    "real": Type.TYPE_NUMBER,  # FLOAT4OID
    "double precision": Type.TYPE_NUMBER,  # FLOAT8OID
    "smallint": Type.TYPE_INTEGER,  # INT2OID
    "integer": Type.TYPE_INTEGER,  # INT4OID
    "int4": Type.TYPE_INTEGER,  # INT4OID
    "int8": Type.TYPE_INTEGER,  # INT4OID
    "bigint": Type.TYPE_INTEGER,  # INT8OID recheck
    "numeric": Type.TYPE_NUMBER,  # NUMERICOID
    "interval": Type.TYPE_DURATION,  # INTERVALOID recheck
    "time": Type.TYPE_TIME,  # TIMEOID, TIMETZOID
    "time without time zone": Type.TYPE_TIME,  # TIMEOID
    "time with time zone": Type.TYPE_TIME,  # TIMETZOID
    "timestamp": Type.TYPE_DATETIME,  # TIMESTAMPOID, TIMESTAMPTZOID
    "timestamp without time zone": Type.TYPE_DATETIME,  # TIMESTAMPOID
    "timestamp with time zone": Type.TYPE_DATETIME,  # TIMESTAMPTZOID
    "bit varying": Type.TYPE_BINARY,  # VARBITOID recheck
    "character varying": Type.TYPE_STRING,  # VARCHAROID
    "bytea": Type.TYPE_BINARY,
    "ARRAY": Type.TYPE_LIST,  # view information_schema.columns recheck
}

TABLE_TYPES_SQL_TO_ODD: Dict[str, DataEntityType] = {
    "BASE TABLE": DataEntityType.TABLE,
    # 'FOREIGN': DataEntityType.DATASET_TABLE,
    # 'LOCAL TEMPORARY': DataEntityType.DATASET_TABLE,
    "VIEW": DataEntityType.VIEW,
}
