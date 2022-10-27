from collections import namedtuple

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/snowflake.json#/definitions/Snowflake"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)

_data_set_metadata_excluded_keys: set = {
    "table_type",
    "table_owner",
    "row_count",
    "comment",
    "last_altered",
    "created",
}

_table_metadata: str = (
    "table_catalog, table_schema, table_name, table_owner, table_type, is_transient, clustering_key, "
    "row_count, bytes, retention_time, created, last_altered, auto_clustering_on, comment, view_definition, is_secure"
)

_tables_select = """
SELECT
       t.TABLE_CATALOG,
       t.TABLE_SCHEMA,
       t.TABLE_NAME,
       t.TABLE_OWNER,
       t.TABLE_TYPE,
       t.IS_TRANSIENT,
       t.CLUSTERING_KEY,
       t.ROW_COUNT,
       t.BYTES,
       t.RETENTION_TIME,
       t.CREATED,
       t.LAST_ALTERED,
       t.AUTO_CLUSTERING_ON,
       t.COMMENT,
       v.VIEW_DEFINITION,
       v.IS_SECURE
FROM INFORMATION_SCHEMA.TABLES as t
LEFT JOIN INFORMATION_SCHEMA.VIEWS as v on (
    v.TABLE_CATALOG = t.TABLE_CATALOG and v.TABLE_SCHEMA = t.TABLE_SCHEMA and v.TABLE_NAME = t.TABLE_NAME
)
WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME;
"""

_column_metadata: str = (
    "table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, "
    "is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, "
    "numeric_precision_radix, numeric_scale, collation_name, is_identity, identity_generation, "
    "identity_start, identity_increment, identity_cycle, comment"
)

_data_set_field_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
    "comment",
}

_columns_select = """
SELECT
       c.TABLE_CATALOG,
       c.TABLE_SCHEMA,
       c.TABLE_NAME,
       c.COLUMN_NAME,
       c.ORDINAL_POSITION,
       c.COLUMN_DEFAULT,
       c.IS_NULLABLE,
       c.DATA_TYPE,
       c.CHARACTER_MAXIMUM_LENGTH,
       c.CHARACTER_OCTET_LENGTH,
       c.NUMERIC_PRECISION,
       c.NUMERIC_PRECISION_RADIX,
       c.NUMERIC_SCALE,
       c.COLLATION_NAME,
       c.IS_IDENTITY,
       c.IDENTITY_GENERATION,
       c.IDENTITY_START,
       c.IDENTITY_INCREMENT,
       c.IDENTITY_CYCLE,
       c.COMMENT
FROM INFORMATION_SCHEMA.COLUMNS as c
JOIN INFORMATION_SCHEMA.TABLES as t on (
    c.TABLE_CATALOG = t.TABLE_CATALOG and c.TABLE_SCHEMA = t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME
)
WHERE c.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
"""

MetadataNamedtuple = namedtuple("MetadataNamedtuple", _table_metadata)
ColumnMetadataNamedtuple = namedtuple("ColumnMetadataNamedtuple", _column_metadata)
