from typing import Dict, List

from odd_models.models import DataEntity, DataEntityType, DataSet
from .fields import map_field
from ..logger import logger
from . import metadata_extractor


def get_template_structure(template_data: List, oddrn_generator):
    logger.debug(f"Lenght of template data is {len(template_data)}")

    if len(template_data) != 1:
        raise Exception("Expected one Data Stream has one Data Stream Template")

    template_name = template_data[0]["name"]
    oddrn_generator.set_oddrn_paths(indexes=template_name)

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
    return fields


def map_data_stream(stream_data, template_data, lifecycle_policies, oddrn_generator):
    data_stream_fields = get_template_structure(template_data, oddrn_generator)

    field_name = stream_data["name"]
    oddrn_generator.set_oddrn_paths(indexes=field_name)
    index_oddrn = oddrn_generator.get_oddrn_by_path("indexes")

    logger.debug(f"Map Stream {stream_data} with lifecycle policies {lifecycle_policies}")

    metadata = metadata_extractor.extract_index_metadata(stream_data)
    if lifecycle_policies:
        metadata['metadata'].update(lifecycle_policies)

    logger.debug(f"Data stream {field_name} has meetadata {metadata}")

    data_stream_entity =  DataEntity(
        oddrn=index_oddrn,
        name=field_name,
        owner=None,
        description="",
        type=DataEntityType.VIEW,
        metadata=[metadata],
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=data_stream_fields),
    )

    return data_stream_entity
