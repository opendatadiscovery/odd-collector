from typing import Optional, List
from oddrn_generator import AirbyteGenerator, Generator
from re import search
from odd_collector.adapters.airbyte.api import AirbyteApi, OddPlatformApi
from odd_collector.adapters.airbyte.mappers.dataset import verify_dataset_name


def generate_connection_oddrn(conn_id: str, oddrn_gen: AirbyteGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("connections", new_value=conn_id)


async def generate_dataset_oddrn(
    is_source: bool,
    connection_meta: dict,
    airbyte_api: AirbyteApi,
    odd_api: OddPlatformApi,
) -> List[Optional[str]]:
    """
    Function intended for generating oddrn of
    sources and destinations in Airbyte connections
    """
    replicated_tables = []
    for stream in connection_meta["syncCatalog"]["streams"]:
        replicated_tables.append(stream["stream"]["name"])

    dataset_id = (
        connection_meta.get("sourceId")
        if is_source
        else connection_meta.get("destinationId")
    )
    dataset_meta = await airbyte_api.get_dataset_definition(
        is_source=is_source, dataset_id=dataset_id
    )

    name = (
        str(dataset_meta.get("sourceName")).lower()
        if is_source
        else str(dataset_meta.get("destinationName")).lower()
    )
    dataset = verify_dataset_name(name)
    entities = []
    if dataset:
        host = dataset_meta.get("connectionConfiguration").get("host")
        database = dataset_meta.get("connectionConfiguration").get("database")
        oddrn_gen = Generator(
            data_source=dataset, host_settings=host, databases=database
        )

        deg_oddrn = oddrn_gen.get_data_source_oddrn()
        dataset_oddrns = await odd_api.get_data_entities_oddrns(deg_oddrn)
        for oddrn in dataset_oddrns:
            if search(r"\w+$", oddrn).group() in replicated_tables:
                entities.append(oddrn)
        return entities
    return entities
