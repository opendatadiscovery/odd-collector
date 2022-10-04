from typing import Dict
import logging
from urllib.parse import urlparse

from oddrn_generator import AirbyteGenerator


def generate_connection_oddrn(conn_id: str, oddrn_gen: AirbyteGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("connections", new_value=conn_id)


def generate_dataset_oddrn(dataset_id: str, oddrn_gen: AirbyteGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("datasets", new_value=dataset_id)
