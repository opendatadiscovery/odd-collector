from typing import Dict

from odd_models.models import DataEntity, DataEntityType, DataSet
from .fields import map_field
from ..logger import logger


def map_stream(stream_data: Dict, oddrn_generator):
    field_name = stream_data["name"]
    oddrn_generator.set_oddrn_paths(indexes=field_name)
    index_oddrn = oddrn_generator.get_oddrn_by_path("indexes")

    logger.debug(f"Map Stream {stream_data}")
    dataset_field = map_field(field_name, stream_data, oddrn_generator, True)

    # TODO: add temaplates and rollover rule in metadata
    logger.debug(f"Created Dataset Field {dataset_field} for {field_name}")
    logger.debug(type(dataset_field))
    return DataEntity(
        oddrn=index_oddrn,
        name=field_name,
        owner=None,
        description="",
        type=DataEntityType.TABLE,
        metadata=None,
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=[dataset_field])
    )

