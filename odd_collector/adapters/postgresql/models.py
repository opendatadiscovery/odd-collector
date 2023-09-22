from dataclasses import dataclass, field
from typing import Any
from funcy import omit
from sql_metadata import Parser

from .logger import logger
from odd_collector_sdk.utils.metadata import HasMetadata


@dataclass
class Dependency:
    name: str
    schema: str

    @property
    def uid(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class EnumTypeLabel:
    type_oid: int
    type_name: str
    label: str


@dataclass(frozen=True)
class PrimaryKey:
    table_name: str
    column_name: str
    attrelid: int


@dataclass
class Column:
    attrelid: int
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_precision_radix: Any
    numeric_scale: Any
    datetime_precision: Any
    interval_type: Any
    interval_precision: Any
    character_set_catalog: Any
    character_set_schema: Any
    character_set_name: Any
    collation_catalog: Any
    collation_schema: Any
    collation_name: Any
    domain_catalog: Any
    domain_schema: Any
    domain_name: Any
    udt_catalog: Any
    udt_schema: Any
    udt_name: Any
    scope_catalog: Any
    scope_schema: Any
    scope_name: Any
    maximum_cardinality: Any
    dtd_identifier: Any
    is_self_referencing: Any
    is_identity: Any
    identity_generation: Any
    identity_start: Any
    identity_increment: Any
    identity_maximum: Any
    identity_minimum: Any
    identity_cycle: Any
    is_generated: Any
    generation_expression: Any
    is_updatable: Any
    description: Any
    type_oid: int
    enums: list[EnumTypeLabel] = field(default_factory=list)
    is_primary: bool = False

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "column_default",
                "is_nullable",
                "data_type",
                "description",
            },
        )


@dataclass(frozen=True)
class Table:
    oid: int
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_type: Any
    self_referencing_column_name: Any
    reference_generation: Any
    user_defined_type_catalog: Any
    user_defined_type_schema: Any
    user_defined_type_name: Any
    is_insertable_into: Any
    is_typed: Any
    commit_action: Any
    view_definition: Any
    view_check_option: Any
    view_is_updatable: Any
    view_is_insertable_into: Any
    view_is_trigger_updatable: Any
    view_is_trigger_deletable: Any
    view_is_trigger_insertable_into: Any
    table_owner: Any
    table_rows: Any
    description: Any
    columns: list[Column] = field(default_factory=list)
    primary_keys: list[PrimaryKey] = field(default_factory=list)

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "view_definition",
                "table_owner",
                "table_rows",
                "description",
            },
        )

    @property
    def as_dependency(self) -> Dependency:
        return Dependency(name=self.table_name, schema=self.table_schema)

    @property
    def dependencies(self) -> list[Dependency]:
        try:
            if not self.view_definition:
                return []

            parsed = Parser(self.view_definition.replace("(", "").replace(")", ""))
            dependencies = []

            for table in parsed.tables:
                schema_name = table.split(".")

                if len(schema_name) > 2:
                    logger.warning(
                        f"Couldn't parse schema and name from {table}. Must be in format <schema>.<table> or <table>."
                    )
                    continue

                if len(schema_name) == 2:
                    schema, name = schema_name
                else:
                    schema, name = "public", schema_name[0]

                dependencies.append(Dependency(name=name, schema=schema))
            return dependencies
        except Exception as e:
            logger.exception(
                f"Couldn't parse dependencies from {self.view_definition}. {e}"
            )
            return []


@dataclass(frozen=True)
class Schema(HasMetadata):
    schema_name: str
    schema_owner: str
    oid: int
    description: str
    total_size_bytes: int

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {"schema_name"},
        )
