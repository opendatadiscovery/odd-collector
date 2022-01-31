import os
import sys
from types import ModuleType
import pydantic
import yaml
import logging

from importlib import util
from importlib import import_module
from typing import List, Tuple
from typing import Set
from typing import Optional

from odd_collector.domain.collector_config import CollectorConfig
from odd_collector.domain.plugin import Plugin


def get_config(path: str) -> CollectorConfig:
    """Opening the config file and returning the list of modules to import"""
    with open(path, "r") as stream:
        try:
            parsed_yaml_file = yaml.safe_load(stream)
            print(parsed_yaml_file)
            return CollectorConfig.parse_obj(parsed_yaml_file)
        except (yaml.YAMLError, pydantic.ValidationError) as e:
            logging.error(e)


def get_odd_packages_name(config: CollectorConfig) -> Set[str]:
    return {plugin.type for plugin in config.plugins}


def check_packages(list_of_custom_modules: Set[str]) -> Optional[Set[str]]:
    """Checking if modules have been already imported"""
    if list_of_custom_modules is None:
        return None

    for module_name in list_of_custom_modules:
        if module_name in sys.modules:
            logging.warning(f"module {module_name} has been already imported")
            list_of_custom_modules.remove(module_name)
        elif util.find_spec(module_name) is None:
            logging.warning(f"module {module_name} cannot be found")
            list_of_custom_modules.remove(module_name)

    return list_of_custom_modules


def import_odd_packages(
    config: CollectorConfig,
) -> Optional[List[ModuleType]]:
    """Importing the packages that are provided in the config.yaml file"""
    packages_to_import = check_packages(get_odd_packages_name(config))

    if packages_to_import is None:
        return None

    imported_packages = [import_module(package) for package in packages_to_import]
    return imported_packages


def import_odd_package_modules(package: ModuleType) -> List[ModuleType]:
    package_name = package.__name__
    package_path = package.__path__[0]

    underlying_modules = []

    package_module_files = [
        os.path.join(root, file)
        for root, dirs, files in os.walk(package_path)
        for file in files
        if file.endswith(".py") and not file.endswith("__init__.py")
    ]

    for file_name in package_module_files:
        modules = file_name.split(".")[0].split("/")
        modules = modules[modules.index(package_name) :]
        modules = ".".join(modules)
        underlying_modules.append(modules)

    return [import_module(module) for module in underlying_modules]


def load_plugins_packages(config: CollectorConfig) -> List[Tuple[ModuleType, Plugin]]:
    package_with_plugin_config: List[Tuple[ModuleType, Plugin]] = []
    plugins_loaded_package = {}

    for plugin in config.plugins:
        if plugin.type not in plugins_loaded_package:
            logging.warning(f"module {plugin.type} has been already imported")
            imported_package = import_module(plugin.type)
            import_odd_package_modules(imported_package)

            plugins_loaded_package[plugin.type] = imported_package

        package = plugins_loaded_package[plugin.type]
        package_with_plugin_config.append((package, plugin))

    return package_with_plugin_config
