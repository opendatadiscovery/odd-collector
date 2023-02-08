from typing import Any, Dict

from odd_models.models import DataSetField, DataSetFieldType, Type

# As of ElasticSearch 7.x supported fields are listed here
# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html#
TYPES_ELASTIC_TO_ODD = {
    "blob": "TYPE_STRING",
    "boolean": "TYPE_BOOLEAN",
    "constant_keyword": "TYPE_STRING",
    "date": "TYPE_DATETIME",
    "date_nanos": "TYPE_INTEGER",
    "double": "TYPE_NUMBER",
    "float": "TYPE_NUMBER",
    "geo_point": "TYPE_MAP",
    "flattened": "TYPE_MAP",
    "half_float": "TYPE_NUMBER",
    "integer": "TYPE_INTEGER",
    "ip": "TYPE_STRING",
    "keyword": "TYPE_STRING",
    "long": "TYPE_INTEGER",
    "nested": "TYPE_LIST",
    "object": "TYPE_MAP",
    "text": "TYPE_STRING",
    "wildcard": "TYPE_STRING",
}


def is_logical(type_property: str) -> bool:
    return type_property == "boolean"


def __get_field_type(props: Dict[str, Any]):
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
    if "type" in props:
        return props["type"]
    if "properties" in props:
        return "object"
    return None


def map_field(field_name, field_metadata: dict, oddrn_generator) -> DataSetField:
    data_type: str = __get_field_type(field_metadata)

    oddrn_path: str = oddrn_generator.get_oddrn_by_path("fields", field_name)

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_path,
        name=field_name,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_ELASTIC_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=is_logical(data_type),
            is_nullable=True,
        ),
        default_value=None,
        description=None,
    )

    return dsf
