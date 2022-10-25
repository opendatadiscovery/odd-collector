from collections import namedtuple
from typing import Dict, List, NamedTuple

from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import Neo4jGenerator

from . import (
    FieldMetadata,
    NodeMetadata,
    RelationMetadata,
    _data_set_metadata_excluded_keys,
    _data_set_metadata_schema_url,
)
from .fields import map_field
from .metadata import append_metadata_extension


def map_nodes(
    oddrn_generator: Neo4jGenerator, nodes: list, relations: list
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []

    nodes_map: Dict[str, List[NamedTuple]] = {}

    _group_by_labels(nodes_map, NodeMetadata, nodes)

    _group_by_labels(nodes_map, RelationMetadata, relations)

    for node_name in nodes_map:
        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("nodes", node_name),
            name=node_name,
            type=DataEntityType.GRAPH_NODE,
            metadata=[],
        )
        # Dataset
        data_entity.dataset = DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("databases"), field_list=[]
        )
        fields: set = set()
        for metadata in nodes_map[node_name]:
            items = metadata._asdict()
            if "properties" in items:
                fields = set.union(fields, set(items["properties"]))
            append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url,
                metadata,
                _data_set_metadata_excluded_keys,
            )

        for field in [(field_name, "string") for field_name in fields]:
            meta: FieldMetadata = FieldMetadata(*field)
            data_entity.dataset.field_list.append(
                map_field(meta, oddrn_generator, data_entity.owner)
            )

        data_entities.append(data_entity)

    return data_entities


def _group_by_labels(
    nodes_map: Dict[str, List[NamedTuple]], namedtuple_func: namedtuple, items: list
):
    for node in items:
        metadata: NamedTuple = namedtuple_func(*node)
        node_name: str = _get_node_name(metadata.node_labels)
        n = nodes_map.get(node_name)
        if n:
            n.append(metadata)
        else:
            nodes_map[node_name] = [metadata]


def _get_node_name(node_labels):
    _res = ""
    for label in node_labels:
        if _res != "":
            _res += ":"
        _res += label
    return _res
