from os import path
import pydantic
import yaml
import sys
import pytest
from odd_collector.domain.plugin import DynamoDbPlugin, GluePlugin

from odd_collector.domain.collector_config import CollectorConfig
from odd_collector.domain.collector_config_loader import CollectorConfigLoader
from odd_collector.domain.adapters_initializer import AdaptersInitializer
from odd_collector.domain.adapters_folder_meta import AdapterFolderMetadata


test_folder_path = path.realpath(path.dirname(__file__))


def test_creating_collector_config():
    empty_config = """
        default_pulling_interval: 10
        token: ""
        plugins:
        - type: glue
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: dynamodb
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
          exclude_tables: []
        - type: athena
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
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
        - type: glue
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: glue
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
    """

    obj = yaml.load(empty_config, yaml.Loader)
    with pytest.raises(pydantic.ValidationError):
        CollectorConfig.parse_obj(obj)


def test_importing_modules():
    config_path = path.join(test_folder_path, "collector_config.yaml")
    load_config = CollectorConfigLoader()
    config = load_config(config_path)

    adapters_meta = AdapterFolderMetadata(
        folder_path=path.join(test_folder_path, "../adapters"),
        root_package="odd_collector.tests.adapters",
    )

    init_adapters = AdaptersInitializer(adapters_meta, config.plugins)

    package_name = "odd_collector.tests.adapters.glue"

    assert package_name not in sys.modules
    assert f"{package_name}.adapter" not in sys.modules

    imported_packages = init_adapters()

    assert len(imported_packages) == 1
    assert package_name in sys.modules
    assert f"{package_name}.adapter" in sys.modules
