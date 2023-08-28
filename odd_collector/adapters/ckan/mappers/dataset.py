from funcy import lpluck_attr
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import CKANGenerator


def map_dataset(
    oddrn_generator: CKANGenerator,
    dataset_raw: dict,
) -> DataEntity:
    name = dataset_raw["name"]
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", name),
        name=name,
        type=DataEntityType.FILE,
        metadata=[],
        dataset=DataSet(field_list=[]),
    )
