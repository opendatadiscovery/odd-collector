from os import path
import pydantic
import yaml
import sys
import pytest
from odd_generic_collector.domain.plugin import MySQLPlugin, ClickhousePlugin

from odd_generic_collector.domain.collector_config import CollectorConfig
from odd_generic_collector.domain.collector_config_loader import CollectorConfigLoader
from odd_generic_collector.domain.adapters_initializer import AdaptersInitializer
from odd_generic_collector.domain.adapters_folder_meta import AdapterFolderMetadata


test_folder_path = path.realpath(path.dirname(__file__))


def test_creating_collector_config():
    empty_config = """
        default_pulling_interval: 10
        platform_host_url: ""
        token: ""
        plugins:
        - type: glue
          name: glue
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: dynamodb
          name: dynamodb
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
          exclude_tables: []
        - type: athena
          name: athena
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
    """

    obj = yaml.load(empty_config, yaml.Loader)
    config = CollectorConfig.parse_obj(obj)
    plugins = config.plugins

    assert isinstance(plugins[0], ClickhousePlugin)
    assert isinstance(plugins[1], MySQLPlugin)


def test_invalid_adapter_name():
    empty_config = """
        default_pulling_interval: 10s
        token: ""
        platform_host_url: ""
        plugins:
        - type: invalid_name
          name: athena
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
        platform_host_url: ""
        plugins:
        - type: glue
          name: glue
          aws_secret_access_key: ""
          aws_access_key_id: ""
          aws_region: ""
        - type: glue
          name: glue
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
        root_package="odd_aws_collector.tests.adapters",
    )

    initializer = AdaptersInitializer(adapters_meta, config.plugins)

    package_name = "odd_aws_collector.tests.adapters.glue"

    assert package_name not in sys.modules
    assert f"{package_name}.adapter" not in sys.modules

    imported_packages = initializer.init_adapters()

    assert len(imported_packages) == 1
    assert package_name in sys.modules
    assert f"{package_name}.adapter" in sys.modules
