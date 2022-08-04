from collections import namedtuple

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/redshift.json#/definitions/Redshift"
)

_data_set_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionBase"
)
_data_set_metadata_schema_url_all: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionAll"
)
_data_set_metadata_schema_url_redshift: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionRedshift"
)
_data_set_metadata_schema_url_external: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionExternal"
)
_data_set_metadata_schema_url_info: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
)

_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtensionBase"
)
_data_set_field_metadata_schema_url_redshift: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)
_data_set_field_metadata_schema_url_external: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtensionExternal"
)

_data_set_metadata_schema_url_function: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionFunction"
)
_data_set_metadata_schema_url_call: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionCall"
)

_data_set_metadata_excluded_keys_info: set = {"database", "schema", "table"}

_data_set_field_metadata_excluded_keys_redshift: set = {
    "database_name",
    "schema_name",
    "table_name",
    "column_name",
    "data_type",
    "column_default",
    "is_nullable",
    "remarks",
}

# Metadata Tables
MetadataNamedtuple = namedtuple(
    typename="MetadataNamedtuple",
    field_names="table_catalog, table_schema, table_name, table_type, remarks",
)
MetadataNamedtuple_QUERY = """
select 
    table_catalog, table_schema, table_name, table_type, remarks
from 
    pg_catalog.svv_tables
where 
    table_schema not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
    and table_schema not like 'pg_temp_%'
    and table_type in ('BASE TABLE', 'VIEW')
order by 
    table_catalog, table_schema, table_name
"""


# Metadata Tables All
MetadataNamedtupleAll = namedtuple(
    typename="MetadataNamedtupleAll",
    field_names="database_name, schema_name, table_name, table_type, table_owner, table_creation_time, view_ddl",
)
MetadataNamedtupleAll_QUERY = """
select 
     (current_database())::character varying(128)             as database_name
     , n.nspname                                                as schema_name
     , c.relname                                                as table_name
     , case when c.relkind = 'v' then 'VIEW' else 'TABLE' end   as table_type
     , pg_get_userbyid(c.relowner)                              as table_owner
     , relcreationtime                                          as table_creation_time
     , case
           when relkind = 'v' then
               coalesce(pg_get_viewdef(c.reloid, true), '') end as view_ddl
from 
    pg_catalog.pg_class_info c
left join 
    pg_catalog.pg_namespace n on n.oid = c.relnamespace
where 
    c.relkind in ('r', 'v')
    and n.nspname not like 'pg_temp_%'
    and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
order by 
    n.nspname, c.relname
"""


# Metadata Tables Redshift
MetadataNamedtupleRedshift = namedtuple(
    typename="MetadataNamedtupleRedshift",
    field_names="database_name, schema_name, table_name, table_type, table_acl, remarks",
)
MetadataNamedtupleRedshift_QUERY = f"""
select 
    database_name, schema_name, table_name, table_type, table_acl, remarks
from 
    pg_catalog.svv_redshift_tables
where 
    schema_name not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
    and schema_name not like 'pg_temp_%'
order by
    database_name, schema_name, table_name
"""


# Metadata Tables External
MetadataNamedtupleExternal = namedtuple(
    typename="MetadataNamedtupleExternal",
    field_names="databasename, schemaname, tablename, location, input_format, output_format, "
    "serialization_lib, serde_parameters, compressed, parameters, tabletype",
)
MetadataNamedtupleExternal_QUERY = """
select 
    (current_database())::character varying(128) as databasename, 
    schemaname, tablename, location, 
    input_format, output_format, serialization_lib, 
    serde_parameters, compressed, parameters, tabletype
from 
    pg_catalog.svv_external_tables 
where 
    schemaname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema') 
    and schemaname not like 'pg_temp_%'
order by 
    schemaname, tablename
"""


# Metadata Tables Info
MetadataNamedtupleInfo = namedtuple(
    typename="MetadataNamedtupleInfo",
    field_names="database, schema, table_id, table, encoded, diststyle, sortkey1, max_varchar, sortkey1_enc, "
    "sortkey_num, size, pct_used, empty, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows, "
    "estimated_visible_rows, risk_event, vacuum_sort_benefit",
)
MetadataNamedtupleInfo_QUERY = """
select 
    database, schema, table_id, "table", encoded, diststyle,
    sortkey1, max_varchar, trim(sortkey1_enc) sortkey1_enc, sortkey_num, 
    size, pct_used, empty, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows, estimated_visible_rows,
    risk_event, vacuum_sort_benefit
from
    pg_catalog.svv_table_info 
where
    schema not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema') 
    and schema not like 'pg_temp_%'
order by
    database, schema, "table"
"""

# Metadata Columns
ColumnMetadataNamedtuple = namedtuple(
    typename="ColumnMetadataNamedtuple",
    field_names="database_name, schema_name, table_name, column_name, ordinal_position, column_default, "
    "is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, remarks",
)
ColumnMetadataNamedtuple_QUERY = """
select 
    c.database_name, c.schema_name, c.table_name, c.column_name, 
    c.ordinal_position, c.column_default, c.is_nullable, c.data_type, 
    c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.remarks
from 
    pg_catalog.svv_all_columns as c
join 
    pg_catalog.svv_tables as t on (
        t.table_catalog = c.database_name and t.table_schema = c.schema_name and t.table_name = c.table_name
    ) 
where 
    c.schema_name not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
    and c.schema_name not like 'pg_temp_%'
    and t.table_type in ('BASE TABLE', 'VIEW') 
order by 
    c.database_name, c.schema_name, c.table_name, c.ordinal_position
"""


# Metadata Columns Redshift
ColumnMetadataNamedtupleRedshift = namedtuple(
    typename="ColumnMetadataNamedtupleRedshift",
    field_names="database_name, schema_name, table_name, column_name, ordinal_position, data_type, column_default, "
    "is_nullable, encoding, distkey, sortkey, column_acl, remarks",
)
ColumnMetadataNamedtupleRedshift_QUERY = """
select 
    database_name, schema_name, table_name, column_name, 
    ordinal_position, data_type, column_default, is_nullable, 
    encoding, distkey, sortkey, column_acl, remarks
from
    pg_catalog.svv_redshift_columns 
where
    schema_name not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema')
    and schema_name not like 'pg_temp_%'
order by 
    database_name, schema_name, table_name, ordinal_position
"""


# Metadata Columns External
ColumnMetadataNamedtupleExternal = namedtuple(
    typename="ColumnMetadataNamedtupleExternal",
    field_names="databasename, schemaname, tablename, columnname, external_type, columnnum, part_key, is_nullable",
)
ColumnMetadataNamedtupleExternal_QUERY = """
select 
    (current_database())::character varying(128) as databasename,
    schemaname, tablename, columnname, external_type, columnnum, part_key, is_nullable
from 
    pg_catalog.svv_external_columns
where 
    schemaname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema') 
    and schemaname not like 'pg_temp_%'
order by 
    schemaname, tablename, columnnum
"""

# Primary Keys Query
PrimaryKeys_Query = """
select 
    kcu.table_name,
	kcu.column_name
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu 
    on kcu.constraint_name = tco.constraint_name
    and kcu.constraint_schema = tco.constraint_schema
    and kcu.constraint_name = tco.constraint_name
where tco.constraint_type = 'PRIMARY KEY'
order by tco.constraint_schema,
         tco.constraint_name,
         kcu.ordinal_position;
"""
