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

# https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
# The following table lists the data types that you can use in Amazon Redshift tables.
# Data type 	    Aliases 	                        Description
# SMALLINT 	        INT2 	                            Signed two-byte integer
# INTEGER 	        INT, INT4 	                        Signed four-byte integer
# BIGINT 	        INT8 	                            Signed eight-byte integer
# DECIMAL 	        NUMERIC 	                        Exact numeric of selectable precision
# REAL 	            FLOAT4 	                            Single precision floating-point number
# DOUBLE PRECISION 	FLOAT8, FLOAT 	                    Double precision floating-point number
# BOOLEAN 	        BOOL 	                            Logical Boolean (true/false)
# CHAR 	            CHARACTER, NCHAR, BPCHAR 	        Fixed-length character string
# VARCHAR 	        CHARACTER VARYING, NVARCHAR, TEXT 	Variable-length character string with a user-defined limit
# DATE 		                                            Calendar date (year, month, day)
# TIMESTAMP 	    TIMESTAMP WITHOUT TIME ZONE 	    Date and time (without time zone)
# TIMESTAMPTZ 	    TIMESTAMP WITH TIME ZONE 	        Date and time (with time zone)
# GEOMETRY 		                                        Spatial data
# HLLSKETCH 		                                    Type used with HyperLogLog sketches.
# TIME 	            TIME WITHOUT TIME ZONE 	            Time of day
# TIMETZ 	        TIME WITH TIME ZONE 	            Time of day with time zone

# https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html

# https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-datatypes.html
from odd_models.models import Type, DataEntityType
from typing import Dict
TYPES_SQL_TO_ODD: Dict[str, Type] = {

    'smallint': Type.TYPE_INTEGER,
    'integer': Type.TYPE_INTEGER,
    'bigint': Type.TYPE_INTEGER,
    'decimal': Type.TYPE_NUMBER,
    'real': Type.TYPE_NUMBER,
    'double precision': Type.TYPE_NUMBER,
    'boolean': Type.TYPE_BOOLEAN,
    'char': Type.TYPE_CHAR,
    'varchar': Type.TYPE_STRING,
    'date': Type.TYPE_DATETIME,
    'timestamp': Type.TYPE_DATETIME,
    'timestamptz': Type.TYPE_DATETIME,
    'geometry': Type.TYPE_BINARY,
    'hllsketch': Type.TYPE_BINARY,
    'time': Type.TYPE_TIME,
    'timetz': Type.TYPE_TIME,

    'bit': Type.TYPE_BINARY,  # BITOID recheck
    # 'boolean': Type.TYPE_BOOLEAN,  # BOOLOID
    'character': Type.TYPE_CHAR,  # BPCHAROID recheck

    # 'real': Type.TYPE_NUMBER,  # FLOAT4OID
    # 'double precision': Type.TYPE_NUMBER,  # FLOAT8OID
    # 'smallint': Type.TYPE_INTEGER,  # INT2OID
    # 'integer': Type.TYPE_INTEGER,  # INT4OID
    # 'bigint': Type.TYPE_NUMBER,  # INT8OID recheck
    'numeric': Type.TYPE_NUMBER,  # NUMERICOID

    'interval': Type.TYPE_DURATION,  # INTERVALOID recheck
    # 'time': Type.TYPE_TIME,  # TIMEOID, TIMETZOID
    'time without time zone': Type.TYPE_TIME,  # TIMEOID
    'time with time zone': Type.TYPE_TIME,  # TIMETZOID
    # 'timestamp': Type.TYPE_DATETIME,  # TIMESTAMPOID, TIMESTAMPTZOID
    'timestamp without time zone': Type.TYPE_DATETIME,  # TIMESTAMPOID
    'timestamp with time zone': Type.TYPE_DATETIME,  # TIMESTAMPTZOID

    'bit varying': Type.TYPE_BINARY,  # VARBITOID recheck
    'character varying': Type.TYPE_STRING,  # VARCHAROID

    'ARRAY': Type.TYPE_LIST,  # view information_schema.columns recheck
    'USER-DEFINED': Type.TYPE_STRUCT,  # view information_schema.columns recheck
}

# views, base tables, external tables, and shared tables
# TABLE, VIEW, MATERIALIZED VIEW, or " " empty string that represents no information.
TABLE_TYPES_SQL_TO_ODD: Dict[str, DataEntityType] = {
    'BASE TABLE': DataEntityType.TABLE,
    'EXTERNAL TABLE': DataEntityType.TABLE,
    'SHARED TABLE': DataEntityType.TABLE,
    'VIEW': DataEntityType.VIEW,
    'MATERIALIZED VIEW': DataEntityType.VIEW,
    'EXTERNAL VIEW': DataEntityType.VIEW,
    'EXTERNAL MATERIALIZED VIEW': DataEntityType.VIEW,

    # '': 'DATASET_UNKNOWN'

    # 'LOCAL TEMPORARY': Type.DATASET_TABLE,
    # 'BASE TABLE': Type.DATASET_TABLE,
    # 'EXTERNAL TABLE': Type.DATASET_EXTERNAL_TABLE,
    # 'EXTERNAL VIEW': Type.DATASET_EXTERNAL_VIEW,
    # 'EXTERNAL MATERIALIZED VIEW': Type.DATASET_EXTERNAL_MATERIALIZED_VIEW,
    # 'SHARED TABLE': Type.DATASET_SHARED_TABLE,
    # 'LOCAL TEMPORARY': Type.DATASET_TEMPORARY_TABLE,
    # 'VIEW': Type.DATASET_VIEW,
    # 'MATERIALIZED VIEW': Type.DATASET_MATERIALIZED_VIEW,
    # '': 'DATASET_UNKNOWN'
}