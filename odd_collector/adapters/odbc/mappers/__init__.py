from collections import namedtuple

_data_set_metadata_schema_url: str = \
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/" \
    "extensions/odbc.json#/definitions/OdbcDataSetExtension"
_data_set_field_metadata_schema_url: str = \
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/" \
    "extensions/odbc.json#/definitions/OdbcDataSetFieldExtension"

_table_metadata: str = "table_cat, table_schem, table_name, table_type, remarks"

_column_metadata: str = \
    "table_cat, table_schem, table_name, column_name, data_type, type_name, column_size, buffer_length, " \
    "decimal_digits, num_prec_radix, nullable, remarks, column_def, sql_data_type, sql_datetime_sub, " \
    "char_octet_length, ordinal_position, is_nullable, " \
    "ss_is_sparse, ss_is_column_set, ss_is_computed, ss_is_identity, " \
    "ss_udt_catalog_name, ss_udt_schema_name, ss_udt_assembly_type_name, " \
    "ss_xml_schemacollection_catalog_name, ss_xml_schemacollection_schema_name, ss_xml_schemacollection_name, " \
    "ss_data_type"

MetadataNamedtuple = namedtuple("MetadataNamedtuple", _table_metadata)
ColumnMetadataNamedtuple = namedtuple("ColumnMetadataNamedtuple", _column_metadata)
