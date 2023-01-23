from abc import ABC
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RedshiftAdapterMetadata(ABC):
    pass


# Metadata Tables
@dataclass(frozen=True)
class MetadataTableBase(RedshiftAdapterMetadata):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_type: Any
    remarks: Any


# Metadata Tables All
@dataclass(frozen=True)
class MetadataTableAll(RedshiftAdapterMetadata):
    database_name: Any
    schema_name: Any
    table_name: Any
    table_type: Any
    table_owner: Any
    table_creation_time: Any
    view_ddl: Any


# Metadata Tables Redshift
@dataclass(frozen=True)
class MetadataTableRedshift(RedshiftAdapterMetadata):
    database_name: Any
    schema_name: Any
    table_name: Any
    table_type: Any
    table_acl: Any
    remarks: Any


# Metadata Tables External
@dataclass(frozen=True)
class MetadataTableExternal(RedshiftAdapterMetadata):
    databasename: Any
    schemaname: Any
    tablename: Any
    location: Any
    input_format: Any
    output_format: Any
    serialization_lib: Any
    serde_parameters: Any
    compressed: Any
    parameters: Any
    tabletype: Any


# Metadata Tables Info
@dataclass(frozen=True)
class MetadataTableInfo(RedshiftAdapterMetadata):
    database: Any
    schema: Any
    table_id: Any
    table: Any
    encoded: Any
    diststyle: Any
    sortkey1: Any
    max_varchar: Any
    sortkey1_enc: Any
    sortkey_num: Any
    size: Any
    pct_used: Any
    empty: Any
    unsorted: Any
    stats_off: Any
    tbl_rows: Any
    skew_sortkey1: Any
    skew_rows: Any
    estimated_visible_rows: Any
    risk_event: Any
    vacuum_sort_benefit: Any


# Metadata Columns
@dataclass(frozen=True)
class MetadataColumnBase(RedshiftAdapterMetadata):
    database_name: Any
    schema_name: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    numeric_precision: Any
    numeric_scale: Any
    remarks: Any


# Metadata Columns Redshift
@dataclass(frozen=True)
class MetadataColumnRedshift(RedshiftAdapterMetadata):
    database_name: Any
    schema_name: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    data_type: Any
    column_default: Any
    is_nullable: Any
    encoding: Any
    distkey: Any
    sortkey: Any
    column_acl: Any
    remarks: Any


# Metadata Columns External
@dataclass(frozen=True)
class MetadataColumnExternal(RedshiftAdapterMetadata):
    databasename: Any
    schemaname: Any
    tablename: Any
    columnname: Any
    external_type: Any
    columnnum: Any
    part_key: Any
    is_nullable: Any
