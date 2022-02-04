import os
from types import ModuleType
import pydantic
import yaml
import logging

from importlib import import_module
from typing import List, Tuple

from odd_collector.domain.collector_config import CollectorConfig
from odd_collector.domain.plugin import Plugin


def get_config(path: str) -> CollectorConfig:
    """Opening the config file and returning the list of modules to import"""
    with open(path, "r") as stream:
        try:
            parsed_yaml_file = yaml.safe_load(stream)
            return CollectorConfig.parse_obj(parsed_yaml_file)
        except (yaml.YAMLError, pydantic.ValidationError) as e:
            logging.error(e)


def file_path_to_module_path(original_file_path: str) -> str:
    without_file_extension = original_file_path.replace('.py', '')
    module_path = without_file_extension.replace('/', '.')
    return module_path


def import_odd_package_modules(package: ModuleType) -> List[ModuleType]:
    package_name = package.__name__
    package_path = package_name.replace('.', '/')

    package_modules = [
        file_path_to_module_path(os.path.join(root, file))
        for root, dirs, files in os.walk(package_path)
        for file in files
        if file.endswith(".py") and not file.endswith("__init__.py")
    ]

    imported = [import_module(module_path) for module_path in package_modules]

    return imported


def load_plugins_packages(config: CollectorConfig) -> List[Tuple[ModuleType, Plugin]]:
    package_with_plugin_config: List[Tuple[ModuleType, Plugin]] = []
    plugins_loaded_package = {}

    for plugin in config.plugins:
        package_path = f"odd_collector.adapters.{plugin.type}"
        if package_path not in plugins_loaded_package:
            imported_package = import_module(f"odd_collector.adapters.{plugin.type}")
            import_odd_package_modules(imported_package)

            plugins_loaded_package[package_path] = imported_package
        else:
            logging.warning(f"package {package_path} has been already imported")

        package = plugins_loaded_package[package_path]
        package_with_plugin_config.append((package, plugin))

    return package_with_plugin_config
