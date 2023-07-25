from funcy import lpluck_attr
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import DuckDBGenerator


def map_schema(
    oddrn_generator: DuckDBGenerator,
    schema_name: str,
    table_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("schemas", schema_name),
        name=schema_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", table_entities)
        ),
    )
