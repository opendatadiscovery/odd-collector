from collections import defaultdict
from typing import Any

from oddrn_generator import CKANGenerator


def get_metadata(data: dict[str, Any], excluded_keys: list[str]) -> dict[str, Any]:
    return {key: data[key] for key in data if key not in excluded_keys}


def group_dataset_oddrns(
    generator: CKANGenerator, dataset: str, groups: list[str], res: defaultdict
):
    oddrn = generator.get_oddrn_by_path("datasets", dataset)
    for group in groups:
        res[group].append(oddrn)
