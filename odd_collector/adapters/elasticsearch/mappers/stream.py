from typing import Dict, List

from odd_models.models import DataEntity, DataEntityType, DataSet
from .fields import map_field
from ..logger import logger


def map_stream(stream_data: Dict, oddrn_generator):
    field_name = stream_data["name"]
    oddrn_generator.set_oddrn_paths(indexes=field_name)
    index_oddrn = oddrn_generator.get_oddrn_by_path("indexes")

    logger.debug(f"Map Stream {stream_data}")

    stream_data_entity =  DataEntity(
        oddrn=index_oddrn,
        name=field_name,
        owner=None,
        description="",
        type=DataEntityType.VIEW,
        metadata=None,
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=[])
    )

    return stream_data_entity


def map_data_stream_template(template_data: List, oddrn_generator):
    logger.debug(f"Lenght of template data is {len(template_data)}")

    if len(template_data) != 1:
        raise Exception("Expected one Data Stream has one Data Stream Template")

    template_name = template_data[0]["name"]
    oddrn_generator.set_oddrn_paths(indexes=template_name)
    index_oddrn = oddrn_generator.get_oddrn_by_path("indexes")

    index_template = template_data[0]['index_template']['template']
    if 'mappings' in index_template:
        mapping = index_template['mappings']
        properties = mapping['properties']

        fields = [
            map_field(field_name, properties[field_name], oddrn_generator)
            for field_name in properties
            if not field_name.startswith("@")
        ]
    else:
        fields = []

    return DataEntity(
        oddrn=index_oddrn,
        name=template_name,
        owner=None,
        description="",
        type=DataEntityType.TABLE,
        metadata=None,
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=fields),
    )


def map_data_stream(stream_data, template_data, oddrn_generator):
    data_stream_entity = map_stream(stream_data, oddrn_generator)
    data_stream_template_entity = map_data_stream_template(template_data, oddrn_generator)

    return [data_stream_entity, data_stream_template_entity]
