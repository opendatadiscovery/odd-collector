from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MssqlGenerator


def extract_table_schema_name(table_entity: DataEntity) -> str:
    return table_entity.metadata[0].metadata["table_schema"]


def map_db_service(
    db_service_name: str,
    group_oddrns: list[str],
    oddrn_path_name: str,
    oddrn_generator: MssqlGenerator,
) -> DataEntity:
    return DataEntity(
        type=DataEntityType.DATABASE_SERVICE,
        name=db_service_name,
        oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path_name, db_service_name),
        data_entity_group=DataEntityGroup(entities_list=group_oddrns),
    )


def extract_schemas_entities_from_tables(
    tables_entities: list[DataEntity], oddrn_generator: MssqlGenerator
) -> list[DataEntity]:
    schemas_list: set[str] = set(
        [extract_table_schema_name(table_entity) for table_entity in tables_entities]
    )
    schemas_tables: list[DataEntity] = []
    for schema_name in schemas_list:
        tables_oddrns: list[str] = []
        for table_entity in tables_entities:
            if extract_table_schema_name(table_entity) == schema_name:
                tables_oddrns.append(table_entity.oddrn)
        schemas_tables.append(
            map_db_service(schema_name, tables_oddrns, "schemas", oddrn_generator)
        )
    return schemas_tables
