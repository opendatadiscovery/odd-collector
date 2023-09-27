from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import ElasticSearchGenerator

from .metadata import extract_data_stream_metadata


def map_data_stream(
    stream_data: dict,
    generator: ElasticSearchGenerator,
) -> DataEntity:
    generator.set_oddrn_paths(streams=stream_data["name"])
    stream_oddrn = generator.get_oddrn_by_path("streams")

    return DataEntity(
        oddrn=stream_oddrn,
        name=stream_data["name"],
        owner=None,
        # TODO: Change to stream type
        type=DataEntityType.FILE,
        metadata=[extract_data_stream_metadata(stream_data)],
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=[]),
    )
