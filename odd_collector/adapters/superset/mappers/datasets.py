from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityType, DataSet

from oddrn_generator.generators import SupersetGenerator
from .columns import map_column
from odd_collector.domain.utils import extract_transformer_data
from ..domain.dataset import Dataset


def create_dataset(oddrn_generator, dataset: Dataset):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("datasets")
    columns = [map_column(oddrn_generator, column) for column in dataset.columns]

    return DataSet(
        parent_oddrn=parent_oddrn,
        field_list=columns,
    )


def map_table(oddrn_generator: SupersetGenerator, dataset: Dataset) -> DataEntity:
    try:
        data_entity = DataEntity(
            oddrn=dataset.get_oddrn(oddrn_generator),
            owner=dataset.owner,
            description=dataset.description,
            metadata=dataset.metadata,
            name=dataset.name,
            type=DataEntityType.UNKNOWN,
            dataset=create_dataset(oddrn_generator, dataset),
        )
        if dataset.kind == "virtual":
            data_entity.type = DataEntityType.VIEW
            data_entity.data_transformer = extract_transformer_data(
                dataset.metadata[0].metadata.get("sql"), oddrn_generator, "datasets"
            )
        else:
            data_entity.type = DataEntityType.TABLE
        return data_entity
    except Exception as e:
        raise MappingDataError(f"Mapping table {dataset.name} failed") from e
