from ..odd_collector.adapters.kafka.kafka_generator import KafkaGenerator
from ..odd_collector.adapters.kafka.mappers.avro_schema import (
    __extract_referenced_nodes,
)
from typing import Dict, Any, List
from confluent_kafka.schema_registry import SchemaRegistryClient
from ..odd_collector.adapters.kafka.mappers.parser import create_mapper


def test_kafka():
    # instance avro_parser
    # instance oddrn_generator

    oddrn_generator = KafkaGenerator(
        host_settings="kafka:9092", clusters="kafka:9092".replace("/", ":")
    )

    schema_client = SchemaRegistryClient({"url": "http://schemaregistry:8085"})

    data = {
        "type": "record",
        "name": "userInfo",
        "namespace": "my.example",
        "fields": [
            {"name": "username", "type": "string", "default": "NONE"},
            {"name": "age", "type": "int", "default": -1},
            {"name": "phone", "type": "string", "default": "NONE"},
            {"name": "housenum", "type": "string", "default": "NONE"},
            {
                "name": "address",
                "type": {
                    "type": "record",
                    "name": "mailing_address",
                    "fields": [
                        {"name": "street", "type": "string", "default": "NONE"},
                        {"name": "city", "type": "string", "default": "NONE"},
                        {"name": "state_prov", "type": "string", "default": "NONE"},
                        {"name": "country", "type": "string", "default": "NONE"},
                        {"name": "zip", "type": "string", "default": "NONE"},
                    ],
                },
            },
        ],
    }

    references: List[Dict[str, Any]] = __extract_referenced_nodes(data, schema_client)

    print(
        create_mapper(oddrn_generator=oddrn_generator, schema_type="AVRO").map_schema(
            data, references
        )
    )
