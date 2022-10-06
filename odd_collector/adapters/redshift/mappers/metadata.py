from collections import namedtuple
from typing import List

from odd_models.models import MetadataExtension

from odd_collector.adapters.redshift.mappers import (
    ColumnMetadataNamedtuple,
    ColumnMetadataNamedtupleExternal,
    ColumnMetadataNamedtupleRedshift,
    MetadataNamedtuple,
    MetadataNamedtupleAll,
    MetadataNamedtupleExternal,
    MetadataNamedtupleInfo,
    MetadataNamedtupleRedshift,
)


class MetadataTable:
    database_name: str
    schema_name: str
    table_name: str
    base: MetadataNamedtuple = None
    all: MetadataNamedtupleAll = None
    redshift: MetadataNamedtupleRedshift = None
    external: MetadataNamedtupleExternal = None
    info: MetadataNamedtupleInfo = None


class MetadataColumn:
    database_name: str
    schema_name: str
    table_name: str
    ordinal_position: int
    base: ColumnMetadataNamedtuple = None
    redshift: ColumnMetadataNamedtupleRedshift = None
    external: ColumnMetadataNamedtupleExternal = None


class MetadataTables:
    items: List[MetadataTable]

    def __init__(
        self,
        tables: List[tuple],
        tables_all: List[tuple],
        tables_redshift: List[tuple],
        tables_external: List[tuple],
        tables_info: List[tuple],
    ) -> None:
        ms: list[MetadataTable] = []
        all_index: int = 0
        redshift_index: int = 0
        external_index: int = 0
        info_index: int = 0

        for table in tables:
            m: MetadataTable = MetadataTable()
            ms.append(m)
            m.base = MetadataNamedtuple(*table)
            m.database_name = m.base.table_catalog
            m.schema_name = m.base.table_schema
            m.table_name = m.base.table_name

            if all_index < len(tables_all):
                mall: MetadataNamedtupleAll = MetadataNamedtupleAll(
                    *tables_all[all_index]
                )
                if (
                    mall.database_name == m.database_name
                    and mall.schema_name == m.schema_name
                    and mall.table_name == m.table_name
                ):
                    m.all = mall
                    all_index += 1

            if redshift_index < len(tables_redshift):
                mredshift: MetadataNamedtupleRedshift = MetadataNamedtupleRedshift(
                    *tables_redshift[redshift_index]
                )
                if (
                    mredshift.database_name == m.database_name
                    and mredshift.schema_name == m.schema_name
                    and mredshift.table_name == m.table_name
                ):
                    m.redshift = mredshift
                    redshift_index += 1

            if external_index < len(tables_external):
                mexternal: MetadataNamedtupleExternal = MetadataNamedtupleExternal(
                    *tables_external[external_index]
                )
                if (
                    mexternal.databasename == m.database_name
                    and mexternal.schemaname == m.schema_name
                    and mexternal.tablename == m.table_name
                ):
                    m.external = mexternal
                    external_index += 1

            if info_index < len(tables_info):
                minfo: MetadataNamedtupleInfo = MetadataNamedtupleInfo(
                    *tables_info[info_index]
                )
                if (
                    minfo.database == m.database_name
                    and minfo.schema == m.schema_name
                    and minfo.table == m.table_name
                ):
                    m.info = minfo
                    info_index += 1

        self.items = ms


class MetadataColumns:
    items: List[MetadataColumn]

    def __init__(
        self,
        columns: List[tuple],
        columns_redshift: List[tuple],
        columns_external: List[tuple],
    ) -> None:
        ms: list[MetadataColumn] = []
        redshift_index: int = 0
        external_index: int = 0

        for column in columns:
            m: MetadataColumn = MetadataColumn()
            ms.append(m)
            m.base = ColumnMetadataNamedtuple(*column)
            m.database_name = m.base.database_name
            m.schema_name = m.base.schema_name
            m.table_name = m.base.table_name
            m.ordinal_position = m.base.ordinal_position

            if redshift_index < len(columns_redshift):
                mredshift: ColumnMetadataNamedtupleRedshift = (
                    ColumnMetadataNamedtupleRedshift(*columns_redshift[redshift_index])
                )
                if (
                    mredshift.database_name == m.database_name
                    and mredshift.schema_name == m.schema_name
                    and mredshift.table_name == m.table_name
                    and mredshift.ordinal_position == m.ordinal_position
                ):
                    m.redshift = mredshift
                    redshift_index += 1

            if external_index < len(columns_external):
                mexternal: ColumnMetadataNamedtupleExternal = (
                    ColumnMetadataNamedtupleExternal(*columns_external[external_index])
                )
                if (
                    mexternal.databasename == m.database_name
                    and mexternal.schemaname == m.schema_name
                    and mexternal.tablename == m.table_name
                    and mexternal.columnnum == m.ordinal_position
                ):
                    m.external = mexternal
                    external_index += 1

        self.items = ms


def _append_metadata_extension(
    metadata_list: List[MetadataExtension],
    schema_url: str,
    named_tuple: namedtuple,
    excluded_keys: set = None,
):
    if named_tuple is not None and len(named_tuple) > 0:
        metadata: dict = named_tuple._asdict()
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None:
                metadata_wo_none[key] = value
        metadata_list.append(
            MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
        )
