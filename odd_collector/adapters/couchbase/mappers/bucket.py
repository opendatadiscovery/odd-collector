from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import CouchbaseGenerator


def map_bucket(
    generator: CouchbaseGenerator, bucket: str, entities: list[str]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("buckets"),
        name=bucket,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
