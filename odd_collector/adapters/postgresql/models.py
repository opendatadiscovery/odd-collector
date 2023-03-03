from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableMetadata:
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


@dataclass(frozen=True)
class PrimaryKey:
    table_name: str
    column_name: str


@dataclass(frozen=True)
class ColumnMetadata:
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


@dataclass(frozen=True)
class EnumTypeLabel:
    type_oid: int
    type_name: str
    label: str
