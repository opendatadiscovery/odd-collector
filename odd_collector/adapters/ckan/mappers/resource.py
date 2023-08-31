from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models import DataEntity, DataEntityType, DataSet
from oddrn_generator import CKANGenerator

from odd_collector.adapters.ckan.mappers.field import map_field
from odd_collector.adapters.ckan.mappers.models import Resource, ResourceField


def map_resource(
    oddrn_generator: CKANGenerator,
    resource: Resource,
    fields: list[ResourceField],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("resources", resource.name),
        name=resource.name,
        type=DataEntityType.FILE,
        metadata=[extract_metadata("ckan", resource, DefinitionType.DATASET_FIELD)],
        dataset=DataSet(
            field_list=[map_field(oddrn_generator, field) for field in fields] if fields else []
        ),
    )
