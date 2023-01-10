from typing import Dict

import pytest
from oddrn_generator.generators import SupersetGenerator

from odd_collector.adapters.superset.domain.dataset import Dataset
from odd_collector.adapters.superset.mappers.databases import map_database

from .raw_data import datasets_nodes


@pytest.fixture
def generator():
    return SupersetGenerator(host_settings="host")


def test_create_databases_entities(generator):
    datasets = [
        Dataset(
            id=dataset.get("id"),
            name=dataset.get("table_name"),
            db_id=dataset.get("database").get("id"),
            db_name=dataset.get("database").get("database_name"),
            kind=dataset.get("kind"),
        )
        for dataset in datasets_nodes
    ]

    databases_ids_names: Dict[int, str] = {
        dataset.database_id: dataset.database_name for dataset in datasets
    }
    databases_entities = [
        map_database(generator, datasets, database_id, database_name)
        for database_id, database_name in databases_ids_names.items()
    ]
    assert len(databases_entities) == 3
    assert databases_entities[0].oddrn == "//superset/host/host/databases/examples"
    assert databases_entities[1].oddrn == "//superset/host/host/databases/jj"
    assert databases_entities[2].oddrn == "//superset/host/host/databases/ppp"

    assert (
        databases_entities[0].data_entity_group.entities_list[0]
        == "//superset/host/host/databases/examples/datasets/threads"
    )

    assert (
        databases_entities[0].data_entity_group.entities_list[1]
        == "//superset/host/host/databases/examples/datasets/pppp"
    )

    assert (
        databases_entities[2].data_entity_group.entities_list[0]
        == "//superset/host/host/databases/ppp/datasets/channels"
    )
