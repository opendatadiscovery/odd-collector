from funcy import lpluck_attr
from odd_models import DataEntity, DataEntityType, DataEntityGroup
from oddrn_generator import RedshiftGenerator


def map_database(
    oddrn_generator: RedshiftGenerator,
    database_name: str,
    schemas_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("databases", database_name),
        name=database_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schemas_entities)
        ),
    )
