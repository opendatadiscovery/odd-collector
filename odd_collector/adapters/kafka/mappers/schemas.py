from odd_models.models import (
    DataEntity,
    DataSet,
    DataEntityType,
    DataEntityGroup,
    DataSetField,
    DataSetFieldType,
)
from oddrn_generator import Generator
from typing import Dict, List
from .avro_schema import avro_schema
from .json_schema import json_schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from odd_models.models import Type


SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/kafka.json"
)


def return_empty(
    data: dict, oddrn_generator: Generator, schema_client: SchemaRegistryClient
) -> List[DataSetField]:
    return []


SCHEMA_MAPPER = {
    "record": avro_schema,
    "object": json_schema,
    "non-registry": return_empty,
}


def dict_to_user(obj, ctx):
    return obj


def map_topics(
    oddrn_generator: Generator,
    topics: List[Dict],
    cluster: str,
    schema_client: SchemaRegistryClient,
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    de_group = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("clusters"),
        name=cluster,
        type=DataEntityType.KAFKA_SERVICE,
        metadata=[],
    )

    for metadata in topics:
        data_entity_type = DataEntityType.KAFKA_TOPIC

        topic: str = metadata["title"]

        if topic in ["_schemas", "__consumer_offsets"]:
            continue

        oddrn_generator.set_oddrn_paths(**{"topics": topic})

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("topics"),
            name=topic,
            type=data_entity_type,
            # TODO add mod and creation dates
            # updated_at=metadata['modification_date'],
            # created_at=metadata['creation_date'],
            metadata=[
                {
                    "schema_url": f"{SCHEMA_FILE_URL}#/definitions/KafkaDataSetExtension",
                    "metadata": metadata["metadata"],
                }
            ],
        )
        data_entity.dataset = DataSet(
            field_list=[],
            parent_oddrn=de_group.oddrn
            # TODO add row number ain mongo
            # rows_number = metadata['row_number']
        )
        value_parcer = SCHEMA_MAPPER.get(metadata["value"]["type"])

        value_field_list = value_parcer(
            metadata["value"], oddrn_generator, schema_client
        )

        data_entity.dataset.field_list = value_field_list

        data_entities.append(data_entity)

    de_group.data_entity_group = DataEntityGroup(
        entities_list=[de.oddrn for de in data_entities]
    )

    data_entities.append(de_group)

    return data_entities
