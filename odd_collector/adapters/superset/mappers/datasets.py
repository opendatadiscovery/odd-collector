from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator.generators import SupersetGenerator
from .columns import map_column
from odd_collector.domain.utils import extract_transformer_data
from ..domain.dataset import Dataset
from oddrn_generator.utils.external_generators import ExternalDbGenerator


def create_dataset(oddrn_generator, dataset: Dataset):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("datasets")
    columns = [map_column(oddrn_generator, column) for column in dataset.columns]

    return DataSet(parent_oddrn=parent_oddrn, field_list=columns)


def map_table(
    oddrn_generator: SupersetGenerator,
    dataset: Dataset,
    external_backend: ExternalDbGenerator = None,
) -> DataEntity:
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
        if external_backend is not None:
            view_gen = external_backend.get_generator_for_schema_lvl(dataset.schema)

            table_path = external_backend.table_path_name
        else:
            view_gen = oddrn_generator
            table_path = "datasets"
        data_entity.type = DataEntityType.VIEW
        data_entity.data_transformer = extract_transformer_data(
            dataset.metadata[0].metadata.get("sql"), view_gen, table_path
        )
    else:
        data_entity.type = DataEntityType.TABLE
    return data_entity
