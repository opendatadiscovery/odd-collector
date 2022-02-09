from typing import Dict, Any

from odd_models.models import DataEntity, DataEntityType

from . import metadata_extractor
from oddrn_generator import QuicksightGenerator


def map_quicksight_data_sources(raw_table_data: Dict[str, Any], account_id: str, region_name: str) -> DataEntity:
    oddrn_gen = QuicksightGenerator(
        cloud_settings={'account': account_id, 'region': region_name},
        data_sources=raw_table_data['DataSourceId']
    )
    return DataEntity(
        oddrn=oddrn_gen.get_oddrn_by_path("data_sources"),
        name=raw_table_data['Name'],
        type=DataEntityType.UNKNOWN,
        created_at=raw_table_data['CreatedTime'].isoformat(),
        metadata=[metadata_extractor.extract_analysis_metadata(raw_table_data)],
    )
