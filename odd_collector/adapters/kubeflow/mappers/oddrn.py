import logging
from typing import Dict
from urllib.parse import urlparse

from oddrn_generator import KubeflowGenerator


def generate_pipeline_oddrn(pip_id: str, oddrn_gen: KubeflowGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("pipelines", new_value=pip_id)


def generate_experiment_oddrn(exp_id: str, oddrn_gen: KubeflowGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("experiments", new_value=exp_id)


def generate_run_oddrn(run_id: str, oddrn_gen: KubeflowGenerator) -> str:
    return oddrn_gen.get_oddrn_by_path("runs", new_value=run_id)


def generate_input_oddrn(parameters: Dict[str, str]) -> str or None:
    try:
        if parameters.get("value", None) is not None:
            value = parameters.get("value")
            url = urlparse(value)
            if url.scheme in ("s3", "gs"):
                return f"/{url.scheme}/{url.netloc}/prefixes{url.path}"
            else:
                return value

    except Exception:
        logging.error("Parameter value is None")
        logging.exception(Exception)
        return None
