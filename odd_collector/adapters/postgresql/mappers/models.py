from dataclasses import dataclass


@dataclass(frozen=True)
class TableMetadata:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str
    self_referencing_column_name: str
    reference_generation: str
    user_defined_type_catalog: str
    user_defined_type_schema: str
    user_defined_type_name: str
    is_insertable_into: str
    is_typed: str
    commit_action: str
    view_definition: str
    view_check_option: str
    view_is_updatable: str
    view_is_insertable_into: str
    view_is_trigger_updatable: str
    view_is_trigger_deletable: str
    view_is_trigger_insertable_into: str
    table_owner: str
    table_rows: str
    description: str


@dataclass(frozen=True)
class ColumnMetadata:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    ordinal_position: str
    column_default: str
    is_nullable: str
    data_type: str
    character_maximum_length: str
    character_octet_length: str
    numeric_precision: str
    numeric_precision_radix: str
    numeric_scale: str
    datetime_precision: str
    interval_type: str
    interval_precision: str
    character_set_catalog: str
    character_set_schema: str
    character_set_name: str
    collation_catalog: str
    collation_schema: str
    collation_name: str
    domain_catalog: str
    domain_schema: str
    domain_name: str
    udt_catalog: str
    udt_schema: str
    udt_name: str
    scope_catalog: str
    scope_schema: str
    scope_name: str
    maximum_cardinality: str
    dtd_identifier: str
    is_self_referencing: str
    is_identity: str
    identity_generation: str
    identity_start: str
    identity_increment: str
    identity_maximum: str
    identity_minimum: str
    identity_cycle: str
    is_generated: str
    generation_expression: str
    is_updatable: str
    description: str
