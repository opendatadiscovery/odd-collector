from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import ElasticSearchGenerator

from odd_collector.adapters.elasticsearch.mappers.fields import map_field
from odd_collector.adapters.elasticsearch.mappers.metadata import extract_index_metadata


def map_index(
    index: dict,
    properties: dict,
    generator: ElasticSearchGenerator,
) -> DataEntity:
    generator.set_oddrn_paths(indices=index["index"])
    index_oddrn = generator.get_oddrn_by_path("indices")

    # field type with `@` prefix defines alias for another field in same index
    field_list = [
        map_field(name, value, generator, "indices_fields")
        for name, value in properties.items()
        if not name.startswith("@")
    ]

    return DataEntity(
        oddrn=index_oddrn,
        name=index["index"],
        owner=None,
        type=DataEntityType.TABLE,
        metadata=[extract_index_metadata(index)],
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=field_list),
    )
