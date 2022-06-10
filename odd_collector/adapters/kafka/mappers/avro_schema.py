from oddrn_generator import Generator
from typing import Dict, List
from odd_models.models import DataSetField
from .parser import create_mapper
from typing import Dict, Any, List, Union
from confluent_kafka.schema_registry import RegisteredSchema, SchemaRegistryClient
import json
import logging as log

def __extract_referenced_nodes(ref_subject: Union[RegisteredSchema, Dict[str, Any]], schema_client: SchemaRegistryClient) -> List[Dict[str, Any]]:
    nodes = []
    references = ref_subject.get('references', []) \
        if isinstance(ref_subject, dict) \
        else ref_subject.schema.references

    for reference in references:
        rs = schema_client.get_version(reference['name'], reference['version'])

        if len(rs.schema.references) > 0:
            nodes.extend(__extract_referenced_nodes(rs, schema_client))

        nodes.append(json.loads(rs.schema.schema_str))

    return nodes

def avro_schema(data: dict, oddrn_generator: Generator, schema_client: SchemaRegistryClient)->List[DataSetField]:

    references: List[Dict[str, Any]] = __extract_referenced_nodes(data, schema_client)
    if references:
        log.debug(f"Found {len(references)} referenced schemas in {data['name']}: {references}")

    return create_mapper(
                        oddrn_generator=oddrn_generator,
                        schema_type=data.get('schemaType', 'AVRO')
                    ).map_schema(data, references)
