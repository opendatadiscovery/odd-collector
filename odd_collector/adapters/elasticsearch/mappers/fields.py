from typing import Any, Dict, Optional

from odd_models.models import DataSetField, DataSetFieldType, Type
from ..logger import logger


# As of ElasticSearch 7.x supported fields are listed here
# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html#
TYPES_ELASTIC_TO_ODD = {
    "blob": Type.TYPE_STRING,
    "boolean": Type.TYPE_BOOLEAN,
    "constant_keyword": Type.TYPE_STRING,
    "date": Type.TYPE_DATETIME,
    "date_nanos": Type.TYPE_INTEGER,
    "double": Type.TYPE_NUMBER,
    "float": Type.TYPE_NUMBER,
    "geo_point": Type.TYPE_MAP,
    "flattened": Type.TYPE_MAP,
    "half_float": Type.TYPE_NUMBER,
    "integer": Type.TYPE_INTEGER,
    "ip": Type.TYPE_STRING,
    "keyword": Type.TYPE_STRING,
    "long": Type.TYPE_INTEGER,
    "nested": Type.TYPE_LIST,
    "object": Type.TYPE_MAP,
    "text": Type.TYPE_STRING,
    "wildcard": Type.TYPE_STRING,
}


def is_logical(type_property: str) -> bool:
    return type_property == "boolean"


def __get_field_type(props: Dict[str, Any], is_stream: bool=False) -> str:
    """
    Sample mapping for field types
    {'@timestamp' : {'type' : "alias","path" : "timestamp"},
     'timestamp" : {"type" : "date"},
     'bool_var': {'type': 'boolean'},
     'data_stream': {'properties': {'dataset': {'type': 'constant_keyword'},
                                    'namespace': {'type': 'constant_keyword'},
                                    'type': {'type': 'constant_keyword',
                                            'value': 'logs'}}},
    'event1': {'properties': {'dataset': {'ignore_above': 1024, 'type': 'keyword'}}},
    'event2': {'properties': {'dataset': {'ignore_above': 1024, 'type': 'constant_keyword'}}},
    'event3': {'properties': {'dataset': {'ignore_above': 1024, 'type': 'wildcard'}}},
    'host': {'type': 'object'},
    'int_field': {'type': 'long'},
    'float_field': {'type': 'float'},
    """
    if is_stream:
        return "data_stream"
    elif "type" in props:
        return props["type"]
    elif "properties" in props:
        return "object"
    else:
        return "unknown"


def map_field(field_name, field_metadata: dict, oddrn_generator, is_stream: bool = False) -> DataSetField:
    data_type: str = __get_field_type(field_metadata, is_stream)
    logger.debug(f"Field {field_name} with metadata {field_metadata} has {data_type} type")

    oddrn_path: str = oddrn_generator.get_oddrn_by_path("fields", field_name)
    logger.debug(f"Field {field_name} has oddrn path {oddrn_path}")

    field_type = TYPES_ELASTIC_TO_ODD.get(data_type, Type.TYPE_UNKNOWN) if not is_stream else Type.TYPE_STRUCT

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_path,
        name=field_name,
        metadata=[],
        type=DataSetFieldType(
            type=field_type,
            logical_type=data_type,
            is_nullable=True,
        ),
        default_value=None,
        description=None,
        owner=None,
    )

    return dsf
