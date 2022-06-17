from oddrn_generator import KafkaGenerator

from .abstract_parser import AbstractParser
from .avro_parser import AvroParser
from .json_parser import JsonParser

__MAPPERS = {"AVRO": AvroParser, "JSON": JsonParser}


def create_mapper(oddrn_generator: KafkaGenerator, schema_type: str) -> AbstractParser:
    mapper = __MAPPERS.get(schema_type)
    if mapper is None:
        raise RuntimeError(f"No suitable mapper for the {schema_type} schema")

    return mapper(oddrn_generator)


__all__ = ["create_mapper", "AbstractParser"]
