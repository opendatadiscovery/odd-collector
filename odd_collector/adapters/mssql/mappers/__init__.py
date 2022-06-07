from collections import namedtuple

_data_set_metadata_schema_url: str = \
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/" \
    "extensions/mssql.json#/definitions/MssqlDataSetExtension"
_data_set_field_metadata_schema_url: str = \
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/" \
    "extensions/mssql.json#/definitions/MssqlDataSetFieldExtension"


table_query: str = """
select
       table_catalog,
       table_schema,
       table_name,
       table_type,
       OBJECT_DEFINITION(OBJECT_ID(CONCAT(table_schema, '.', table_name))) as view_definition
from information_schema.tables
order by table_catalog, table_schema, table_name;
"""
_table_metadata: str = "table_catalog, table_schema, table_name, table_type, view_definition"
MetadataNamedtuple = namedtuple("MetadataNamedtuple", _table_metadata)


column_query: str = """
select table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable,
    data_type, character_maximum_length, character_octet_length,
    numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision,
    character_set_catalog, character_set_schema, character_set_name,
    collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name
from information_schema.columns
order by table_catalog, table_schema, table_name, ordinal_position
"""
_column_metadata: str = \
    "table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, " \
    "data_type, character_maximum_length, character_octet_length, " \
    "numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, " \
    "character_set_catalog, character_set_schema, character_set_name, " \
    "collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name"

ColumnMetadataNamedtuple = namedtuple("ColumnMetadataNamedtuple", _column_metadata)
