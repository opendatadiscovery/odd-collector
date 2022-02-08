import yaml
import logging
import pydantic

from .collector_config import CollectorConfig


class CollectorConfigLoader:
    def __call__(self, config_path: str) -> CollectorConfig:
        with open(config_path, "r") as stream:
            try:
                parsed_yaml_file = yaml.safe_load(stream)
                return CollectorConfig.parse_obj(parsed_yaml_file)
            except (yaml.YAMLError, pydantic.ValidationError) as e:
                logging.error(e)
