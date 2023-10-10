from dataclasses import asdict, dataclass, field

from funcy import omit
from odd_collector_sdk.utils.metadata import HasMetadata
from odd_models.models import MetadataExtension
from sqllineage.runner import LineageRunner

from odd_collector.adapters.redshift.mappers.models import (
    MetadataColumnBase,
    MetadataColumnExternal,
    MetadataColumnRedshift,
    MetadataSchemaBase,
    MetadataSchemaExternal,
    MetadataSchemaRedshift,
    MetadataTableAll,
    MetadataTableBase,
    MetadataTableExternal,
    MetadataTableInfo,
    MetadataTableRedshift,
    RedshiftAdapterMetadata,
)

from ..logger import logger
from . import _schema_excluded_keys


@dataclass
class Dependency:
    name: str
    schema: str
    database: str = None

    @property
    def uid(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class MetadataSchema(HasMetadata):
    database_name: str = None
    schema_name: str = None
    base: MetadataSchemaBase = None
    redshift: MetadataSchemaRedshift = None
    external: MetadataSchemaExternal = None

    @property
    def odd_metadata(self) -> dict:
        meta = asdict(self.base)
        return omit(meta, _schema_excluded_keys)


@dataclass
class MetadataColumn:
    database_name: str = None
    schema_name: str = None
    table_name: str = None
    column_name: str = None
    ordinal_position: int = None
    base: MetadataColumnBase = None
    redshift: MetadataColumnRedshift = None
    external: MetadataColumnExternal = None


@dataclass
class MetadataTable:
    database_name: str = None
    schema_name: str = None
    table_name: str = None
    base: MetadataTableBase = None
    all: MetadataTableAll = None
    redshift: MetadataTableRedshift = None
    external: MetadataTableExternal = None
    info: MetadataTableInfo = None
    columns: list[MetadataColumn] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)

    @property
    def as_dependency(self) -> Dependency:
        return Dependency(name=self.table_name, schema=self.schema_name)

    @property
    def dependencies(self) -> list[Dependency]:
        try:
            if not self.all.view_ddl:
                return []

            parsed = LineageRunner(sql=self.all.view_ddl, dialect="redshift")
            dependencies = []

            for table in parsed.source_tables:
                schema_name = table.schema.raw_name.split(".")
                name = table.raw_name

                if len(schema_name) == 1:
                    schema = schema_name[0]
                    dependencies.append(
                        Dependency(
                            name=name, schema=schema, database=self.database_name
                        )
                    )
                elif len(schema_name) == 2:
                    dbname, schema = schema_name
                    dependencies.append(
                        Dependency(name=name, schema=schema, database=dbname)
                    )
                else:
                    logger.debug(self.all.view_ddl)
                    logger.warning(
                        f"Couldn't get dependencies {table}. Wrong schema name format."
                    )
                    continue

            return dependencies
        except Exception as e:
            logger.debug(self.all.view_ddl)
            logger.warning(
                f"Couldn't parse dependencies {self.database_name}.{self.schema_name}.{self.table_name}. {e}"
            )
            return []


class MetadataSchemas:
    items: list[MetadataSchema]

    def __init__(
        self,
        schemas_base: list[tuple],
        schemas_redshift: list[tuple],
        schemas_external: list[tuple],
    ) -> None:
        ms: list[MetadataSchema] = []
        redshift_index: int = 0
        external_index: int = 0

        for schema in schemas_base:
            m: MetadataSchema = MetadataSchema()
            ms.append(m)
            m.base = MetadataSchemaBase(*schema)
            m.database_name = m.base.database_name
            m.schema_name = m.base.schema_name

            if redshift_index < len(schemas_redshift):
                mredshift: MetadataSchemaRedshift = MetadataSchemaRedshift(
                    *schemas_redshift[redshift_index]
                )
                if (
                    mredshift.database_name == m.database_name
                    and mredshift.schema_name == m.schema_name
                ):
                    m.redshift = mredshift
                    redshift_index += 1

            if external_index < len(schemas_external):
                mexternal: MetadataSchemaExternal = MetadataSchemaExternal(
                    *schemas_external[external_index]
                )
                if (
                    mexternal.databasename == m.database_name
                    and mexternal.schemaname == m.schema_name
                ):
                    m.external = mexternal
                    external_index += 1

        self.items = ms


class MetadataTables:
    items: list[MetadataTable]

    def __init__(
        self,
        tables_base: list[tuple],
        tables_all: list[tuple],
        tables_redshift: list[tuple],
        tables_external: list[tuple],
        tables_info: list[tuple],
    ) -> None:
        ms: list[MetadataTable] = []
        all_index: int = 0
        redshift_index: int = 0
        external_index: int = 0
        info_index: int = 0

        for table in tables_base:
            m: MetadataTable = MetadataTable()
            ms.append(m)
            m.base = MetadataTableBase(*table)
            m.database_name = m.base.table_catalog
            m.schema_name = m.base.table_schema
            m.table_name = m.base.table_name

            if all_index < len(tables_all):
                mall: MetadataTableAll = MetadataTableAll(*tables_all[all_index])
                if (
                    mall.database_name == m.database_name
                    and mall.schema_name == m.schema_name
                    and mall.table_name == m.table_name
                ):
                    m.all = mall
                    all_index += 1

            if redshift_index < len(tables_redshift):
                mredshift: MetadataTableRedshift = MetadataTableRedshift(
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
                mexternal: MetadataTableExternal = MetadataTableExternal(
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
                minfo: MetadataTableInfo = MetadataTableInfo(*tables_info[info_index])
                if (
                    minfo.database == m.database_name
                    and minfo.schema == m.schema_name
                    and minfo.table == m.table_name
                ):
                    m.info = minfo
                    info_index += 1

        self.items = ms


class MetadataColumns:
    items: list[MetadataColumn]

    def __init__(
        self,
        columns: list[tuple],
        columns_redshift: list[tuple],
        columns_external: list[tuple],
    ) -> None:
        ms: list[MetadataColumn] = []
        redshift_index: int = 0
        external_index: int = 0

        for column in columns:
            m: MetadataColumn = MetadataColumn()
            ms.append(m)
            m.base = MetadataColumnBase(*column)
            m.database_name = m.base.database_name
            m.schema_name = m.base.schema_name
            m.table_name = m.base.table_name
            m.column_name = m.base.column_name
            m.ordinal_position = m.base.ordinal_position

            if redshift_index < len(columns_redshift):
                mredshift: MetadataColumnRedshift = MetadataColumnRedshift(
                    *columns_redshift[redshift_index]
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
                mexternal: MetadataColumnExternal = MetadataColumnExternal(
                    *columns_external[external_index]
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
    metadata_list: list[MetadataExtension],
    schema_url: str,
    metadata_dataclass: RedshiftAdapterMetadata,
    excluded_keys: set = None,
):
    if metadata_dataclass:
        metadata: dict = asdict(metadata_dataclass)
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
