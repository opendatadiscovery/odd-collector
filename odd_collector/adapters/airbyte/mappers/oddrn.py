from typing import Optional
from oddrn_generator import AirbyteGenerator, Generator
from odd_collector.adapters.airbyte.mappers.dataset import verify_dataset_name


def generate_connection_oddrn(conn_id: str, oddrn_gen: AirbyteGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("connections", new_value=conn_id)


def generate_dataset_oddrn(is_source: bool, dataset_meta: dict) -> Optional[str]:
    """Function intended for generatring oddrn of
    sources and destinations in Airbyte connections"""

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
    return None
