from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models import DataEntity, DataEntityType, DataSet
from oddrn_generator import CKANGenerator

from odd_collector.adapters.ckan.mappers.models import CKANResource


def map_resource(
    oddrn_generator: CKANGenerator,
    resource: CKANResource,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", resource.name),
        name=resource.name,
        type=DataEntityType.FILE,
        metadata=[extract_metadata("ckan", resource, DefinitionType.DATASET_FIELD)],
        dataset=DataSet(field_list=[]),
    )
