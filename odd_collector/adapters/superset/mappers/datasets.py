from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityType, DataSet

from odd_collector.adapters.superset.plugin.plugin import SupersetGenerator
from .columns import map_column
from ..domain.dataset import Dataset


def create_dataset(oddrn_generator, dataset: Dataset):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("datasets")
    columns = [map_column(oddrn_generator, column) for column in dataset.columns]

    return DataSet(
        parent_oddrn=parent_oddrn,
        field_list=columns,
    )


def map_table(oddrn_generator: SupersetGenerator, dataset: Dataset) -> DataEntity:
    if dataset.kind == 'virtual':
        _type = DataEntityType.VIEW
    else:
        _type = DataEntityType.TABLE
    try:
        return DataEntity(
            oddrn=dataset.get_oddrn(oddrn_generator),
            name=dataset.name,
            type=_type,
            dataset=create_dataset(oddrn_generator, dataset),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping table {dataset.name} failed") from e
