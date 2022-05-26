from typing import Dict, Any

from odd_models.models import DataEntity, DataEntityType
from oddrn_generator import KinesisGenerator
from . import metadata_extractor


def map_kinesis_stream(raw_stream_data: Dict[str, Any],
                       oddrn_generator: KinesisGenerator) -> DataEntity:
    """
    :param raw_stream_data: dictionary containing raw information about the stream
    :param oddrn_generator: the oddrn generator of the adapter
    :return: DataEntity object containing the mapped information from the stream
    """
    oddrn_generator.set_oddrn_paths(streams=raw_stream_data['StreamName'])
    stream_oddrn = oddrn_generator.get_oddrn_by_path('streams')

    return DataEntity(
        oddrn=stream_oddrn,
        name=raw_stream_data['StreamName'],
        type=DataEntityType.KAFKA_TOPIC,
        metadata=[metadata_extractor.extract_data_entity_metadata(raw_stream_data)]
    )
