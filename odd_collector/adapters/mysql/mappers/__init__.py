from collections import namedtuple

_METADATA_SCHEMA_URL_PREFIX: str = \
    'https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/' \
    'extensions/mysql.json#/definitions/Mysql'

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + 'DataSetExtension'
_data_set_field_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + 'DataSetFieldExtension'

_data_set_metadata_excluded_keys: set = \
    {'table_catalog', 'table_schema', 'table_name', 'table_type',
     'table_rows', 'create_time', 'update_time', 'table_comment', 'view_definition'}

_data_set_field_metadata_excluded_keys: set = \
    {'table_catalog', 'table_schema', 'table_name', 'column_name',
     'data_type', 'column_default', 'is_nullable', 'column_type', 'column_comment'}

_table_metadata: str = \
    'table_catalog, table_schema, table_name, table_type, engine, version, row_format, table_rows, ' \
    'avg_row_length, data_length, max_data_length, index_length, data_free, auto_increment, create_time, ' \
    'update_time, check_time, table_collation, checksum, create_options, table_comment, view_definition'

_table_select = '''
select t.table_catalog,
       t.table_schema,
       t.table_name,
       t.table_type,
       t.engine,
       t.version,
       t.row_format,
       t.table_rows,
       t.avg_row_length,
       t.data_length,
       t.max_data_length,
       t.index_length,
       t.data_free,
       t.auto_increment,
       t.create_time,
       t.update_time,
       t.check_time,
       t.table_collation,
       t.checksum,
       t.create_options,
       t.table_comment,
       v.view_definition
from information_schema.tables t
         left join information_schema.views v
                   on t.TABLE_CATALOG = v.TABLE_CATALOG and
                      t.TABLE_SCHEMA = v.TABLE_SCHEMA and
                      t.TABLE_NAME = v.TABLE_NAME
where t.table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
order by t.table_catalog, t.table_schema, t.table_name
'''

_column_metadata: str = \
    'table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, ' \
    'data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, ' \
    'datetime_precision, character_set_name, collation_name, column_type, column_key, extra, privileges, ' \
    'column_comment, generation_expression'  # , srs_id'  # srs_id column is not included in MariaDB (MySQL only)
_column_table: str = \
    'information_schema.columns ' \
    'where table_schema not in (\'information_schema\', \'mysql\', \'performance_schema\', \'sys\')'
_column_order_by: str = 'table_catalog, table_schema, table_name, ordinal_position'

MetadataNamedtuple = namedtuple('MetadataNamedtuple', _table_metadata)
ColumnMetadataNamedtuple = namedtuple('ColumnMetadataNamedtuple', _column_metadata)
