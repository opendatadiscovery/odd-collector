from collections import namedtuple


_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/cassandra.json#/definitions/Cassandra"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)


_data_set_metadata_excluded_keys = ""


_table_metadata: str = (
    "keyspace_name, table_name, additional_write_policy, bloom_filter_fp_chance, caching, cdc, comment, compaction,"
    "compression, crc_check_chance, dclocal_read_repair_chance, default_time_to_live, extensions, flags, "
    "gc_grace_seconds, id, max_index_interval, memtable_flush_period_in_ms, min_index_interval, read_repair, "
    "read_repair_chance, speculative_retry"
)

_table_select: str = """
    SELECT * FROM system_schema.tables 
    WHERE keyspace_name = %(keyspace)s;
"""

_data_set_field_metadata_excluded_keys = ""

_column_metadata: str = "keyspace_name, table_name, column_name, clustering_order, column_name_bytes, kind, position, type"

_column_select: str = """
    SELECT * FROM system_schema.columns 
    WHERE keyspace_name = %(keyspace)s;
"""

TableMetadata = namedtuple("MetadataNamedtuple", _table_metadata)
ColumnMetadata = namedtuple("ColumnMetadataNamedtuple", _column_metadata)
