from os import path
import pydantic
import yaml
import sys
import pytest
from odd_collector.domain import collector_config
from odd_collector.domain.plugin import DynamoDbPlugin, GluePlugin

from odd_collector.module_importer import (
    get_config,
    load_plugins_packages,
)
from odd_collector.domain.collector_config import CollectorConfig

test_folder_path = path.realpath(path.dirname(__file__))


def test_creating_collector_config():
    empty_config = """
        default_pulling_interval: 10s
        token: ""
        plugins:
        - type: odd_glue_adapter
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: odd_dynamodb_adapter
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
          exclude_tables: []
    """

    obj = yaml.load(empty_config, yaml.Loader)
    config = CollectorConfig.parse_obj(obj)
    plugins = config.plugins

    assert isinstance(plugins[0], GluePlugin)
    assert isinstance(plugins[1], DynamoDbPlugin)


def test_invalid_adapter_name():
    empty_config = """
        default_pulling_interval: 10s
        token: ""
        plugins:
        - type: invalid_name
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
    """

    obj = yaml.load(empty_config, yaml.Loader)
    with pytest.raises(pydantic.ValidationError):
        CollectorConfig.parse_obj(obj)


def test_config_with_duplicated_adapter():
    empty_config = """
        default_pulling_interval: 10s
        token: ""
        plugins:
        - type: odd_glue_adapter
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: odd_glue_adapter
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
    """

    obj = yaml.load(empty_config, yaml.Loader)
    with pytest.raises(pydantic.ValidationError):
        CollectorConfig.parse_obj(obj)


def test_importing_modules():
    package_name = "odd_glue_adapter"
    config = get_config(path.join(test_folder_path, "config.yaml"))

    assert package_name not in sys.modules
    assert f"{package_name}.adapter" not in sys.modules

    imported_packages = load_plugins_packages(config)

    assert len(imported_packages) == 2
    assert package_name in sys.modules
    assert f"{package_name}.adapter" in sys.modules
