from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup, DataSetField
from oddrn_generator import Generator
from typing import Dict, List
from .avro_schema import avro_schema
from .json_data import json_data
from .json_schema import json_schema
from .string_data import string_data


SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/" \
                  "main/specification/extensions/kafka.json"
        
SCHEMA_MAPPER = {
    'string':string_data,
    'record':avro_schema,
    'object':json_schema,
    'json'  :json_data
}


def map_topics(oddrn_generator: Generator, topics: List[Dict], cluster: str) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    de_group = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path('clusters'),
        name=cluster,
        type=DataEntityType.KAFKA_SERVICE,
        metadata=[]
        )

    for metadata in topics:
        data_entity_type = DataEntityType.KAFKA_TOPIC

        topic: str = metadata['title']

        oddrn_generator.set_oddrn_paths(**{"topics" : topic})

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("topics"),
            name=topic,
            type=data_entity_type,
            # TODO add mod and creation dates 
            # updated_at=metadata['modification_date'],
            # created_at=metadata['creation_date'],
            metadata=[{'schema_url': f'{SCHEMA_FILE_URL}#/definitions/KafkaDataSetExtension',
                        'metadata': metadata['metadata']}],
        )
        data_entity.dataset = DataSet(
            field_list=[],
            parent_oddrn = de_group.oddrn
            # TODO add row number ain mongo
            # rows_number = metadata['row_number']
        )

        key_parcer = SCHEMA_MAPPER.get(metadata['key']['type'])
        value_parcer = SCHEMA_MAPPER.get(metadata['value']['type'])
        key_field_list = key_parcer({'key':metadata['key']},oddrn_generator)
        value_field_list = value_parcer({'value':metadata['value']},oddrn_generator)

        data_entity.dataset.field_list = value_field_list.extend(key_field_list)
        
        data_entities.append(data_entity)

    de_group.data_entity_group=DataEntityGroup(
            entities_list=[de.oddrn for de in data_entities])

    data_entities.append(de_group)

    return data_entities
