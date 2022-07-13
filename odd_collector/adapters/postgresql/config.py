from dataclasses import make_dataclass

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/postgresql.json#/definitions/Postgresql"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)

_data_set_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "table_type",
    "view_definition",
    "table_owner",
    "table_rows",
    "description",
}


_data_set_field_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
    "description",
}
_table_metadata: str = (
    "table_catalog, table_schema, table_name, table_type, self_referencing_column_name, "
    "reference_generation, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name, "
    "is_insertable_into, is_typed, commit_action, "
    "view_definition, view_check_option, view_is_updatable, view_is_insertable_into, "
    "view_is_trigger_updatable, view_is_trigger_deletable, view_is_trigger_insertable_into, "
    "table_owner, table_rows, description"
)

_column_metadata: str = (
    "table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, "
    "data_type, character_maximum_length, character_octet_length, "
    "numeric_precision, numeric_precision_radix, numeric_scale, "
    "datetime_precision, interval_type, interval_precision, "
    "character_set_catalog, character_set_schema, character_set_name, "
    "collation_catalog, collation_schema, collation_name, "
    "domain_catalog, domain_schema, domain_name, "
    "udt_catalog, udt_schema, udt_name, "
    "scope_catalog, scope_schema, scope_name, "
    "maximum_cardinality, dtd_identifier, is_self_referencing, "
    "is_identity, "
    "identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle, "
    "is_generated, generation_expression, is_updatable, "
    "description"
)


TableMetadata = make_dataclass("MetadataNamedtuple", _table_metadata.split(", "))
ColumnMetadata = make_dataclass(
    "ColumnMetadataNamedtuple", _column_metadata.split(", ")
)
