from typing import Optional
from re import search
from oddrn_generator import AirbyteGenerator, Generator
from odd_collector.domain.plugin import PLUGIN_FACTORY


def generate_connection_oddrn(conn_id: str, oddrn_gen: AirbyteGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("connections", new_value=conn_id)


def generate_deg_oddrn(is_source: bool, dataset_meta: dict) -> Optional[str]:
    """
    Function intended for generating deg oddrns of
    sources and destinations in Airbyte connections
    """

    name = (
        str(dataset_meta.get("sourceName")).lower()
        if is_source
        else str(dataset_meta.get("destinationName")).lower()
    )
    dataset = verify_dataset_name(name)
    if dataset:
        host = dataset_meta.get("connectionConfiguration").get("host")
        database = dataset_meta.get("connectionConfiguration").get("database")
        oddrn_gen = Generator(
            data_source=dataset, host_settings=host, databases=database
        )

        return oddrn_gen.get_data_source_oddrn()


def filter_dataset_oddrn(
    connection_meta: dict, dataset_oddrns: list[str]
) -> list[Optional[str]]:
    """
    Function to filter only replicated data sources
    """
    replicated_tables = []
    for stream in connection_meta["syncCatalog"]["streams"]:
        replicated_tables.append(stream["stream"]["name"])

    entities = []

    for oddrn in dataset_oddrns:
        if search(r"\w+$", oddrn).group() in replicated_tables:
            entities.append(oddrn)
    return entities


def verify_dataset_name(dataset: str) -> Optional[str]:
    """
    Verification if source/destination type in already implemented in ODD
    """
    dataset = "postgresql" if dataset == "postgres" else dataset
    if dataset in PLUGIN_FACTORY.keys():
        return dataset
    else:
        return None
