from collections import namedtuple


_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/neo4j.json#/definitions/Neo4j"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"

_data_set_metadata_excluded_keys: set = {"node_labels"}

_node_metadata: str = "node_labels, nodes_count, properties"

_relation_metadata: str = "node_labels, relation_type, relations_count"

_field_metadata: str = "field_name, data_type"

NodeMetadata = namedtuple("NodeMetadata", _node_metadata)

RelationMetadata = namedtuple("RelationMetadata", _relation_metadata)

FieldMetadata = namedtuple("FieldMetadata", _field_metadata)
