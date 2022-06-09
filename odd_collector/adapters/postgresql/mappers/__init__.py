from collections import namedtuple

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

_table_metadata: str = (
    "table_catalog, table_schema, table_name, table_type, self_referencing_column_name, "
    "reference_generation, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name, "
    "is_insertable_into, is_typed, commit_action, "
    "view_definition, view_check_option, view_is_updatable, view_is_insertable_into, "
    "view_is_trigger_updatable, view_is_trigger_deletable, view_is_trigger_insertable_into, "
    "table_owner, table_rows, description"
)

_table_select = """
select it.table_catalog
     , it.table_schema
     , it.table_name
     , it.table_type
     , it.self_referencing_column_name
     , it.reference_generation
     , it.user_defined_type_catalog
     , it.user_defined_type_schema
     , it.user_defined_type_name
     , it.is_insertable_into
     , it.is_typed
     , it.commit_action
     , iw.view_definition
     , iw.check_option                        as view_check_option
     , iw.is_updatable                        as view_is_updatable
     , iw.is_insertable_into                  as view_is_insertable_into
     , iw.is_trigger_updatable                as view_is_trigger_updatable
     , iw.is_trigger_deletable                as view_is_trigger_deletable
     , iw.is_trigger_insertable_into          as view_is_trigger_insertable_into
     , pg_catalog.pg_get_userbyid(c.relowner) as table_owner
     , c.reltuples                            as table_rows
     , pg_catalog.obj_description(c.oid)      as description
from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace
         join information_schema.tables it on it.table_schema = n.nspname and it.table_name = c.relname
         left join information_schema.views iw on iw.table_schema = n.nspname and iw.table_name = c.relname
where c.relkind in ('r', 'v')
  and n.nspname not like 'pg_temp_%'
  and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
order by n.nspname, c.relname
"""

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

_column_select: str = """
select ic.table_catalog
     , ic.table_schema
     , ic.table_name
     , ic.column_name
     , ic.ordinal_position
     , ic.column_default
     , ic.is_nullable
     , ic.data_type
     , ic.character_maximum_length
     , ic.character_octet_length
     , ic.numeric_precision
     , ic.numeric_precision_radix
     , ic.numeric_scale
     , ic.datetime_precision
     , ic.interval_type
     , ic.interval_precision
     , ic.character_set_catalog
     , ic.character_set_schema
     , ic.character_set_name
     , ic.collation_catalog
     , ic.collation_schema
     , ic.collation_name
     , ic.domain_catalog
     , ic.domain_schema
     , ic.domain_name
     , ic.udt_catalog
     , ic.udt_schema
     , ic.udt_name
     , ic.scope_catalog
     , ic.scope_schema
     , ic.scope_name
     , ic.maximum_cardinality
     , ic.dtd_identifier
     , ic.is_self_referencing
     , ic.is_identity
     , ic.identity_generation
     , ic.identity_start
     , ic.identity_increment
     , ic.identity_maximum
     , ic.identity_minimum
     , ic.identity_cycle
     , ic.is_generated
     , ic.generation_expression
     , ic.is_updatable
     , pg_catalog.col_description(c.oid, a.attnum) as description
from pg_catalog.pg_attribute a
         join pg_catalog.pg_class c on c.oid = a.attrelid
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace
         join information_schema.columns ic on ic.table_schema = n.nspname and ic.table_name = c.relname and
                                               ic.ordinal_position = a.attnum
where c.relkind in ('r', 'v')
  and a.attnum > 0
  and n.nspname not like 'pg_temp_%'
  and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
order by n.nspname, c.relname, a.attnum
"""

MetadataNamedtuple = namedtuple("MetadataNamedtuple", _table_metadata)
ColumnMetadataNamedtuple = namedtuple("ColumnMetadataNamedtuple", _column_metadata)
