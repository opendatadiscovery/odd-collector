from typing import Optional

from odd_collector.domain.plugin import PLUGIN_FACTORY


def verify_dataset_name(dataset: str) -> Optional[str]:
    """
    Verification if source/destination type in already implemented in ODD
    """
    dataset = "postgresql" if dataset == "postgres" else dataset
    if dataset in PLUGIN_FACTORY.keys():
        return dataset
    else:
        return None
