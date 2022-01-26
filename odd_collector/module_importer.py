import os
from importlib import util
from importlib import import_module
import sys
import yaml
import logging


def parse_config_file(yaml_dict: dict, parent_id: str = 'package', package_id: str = "name") -> list:
    """Return the list of modules to import"""
    return [names[package_id] for names in yaml_dict[parent_id]]


def open_config_module_file(path: str) -> list:
    """Opening the config file and returning the list of modules to import"""
    with open(path, "r") as stream:
        try:
            parsed_yaml_file = yaml.safe_load(stream)
            package_list = parse_config_file(parsed_yaml_file)
            return package_list
        except yaml.YAMLError as e:
            logging.error(e)


def check_packages(list_of_custom_modules: list) -> list:
    """Checking if modules have been already imported"""
    if list_of_custom_modules is not None:
        # deletes duplicates
        list_of_custom_modules = list(dict.fromkeys(list_of_custom_modules))
        for i in list_of_custom_modules:
            if i in sys.modules:
                logging.warning(f"module {i} has been already imported")
                list_of_custom_modules.remove(i)
            elif util.find_spec(i) is None:
                logging.warning(f"module {i} cannot be found")
                list_of_custom_modules.remove(i)
        return list_of_custom_modules


def import_custom_packages(path: str):
    """Importing the modules that are provided in the config.yaml file in the package parent"""
    list_of_custom_modules_to_import = check_packages(open_config_module_file(path=path))
    if list_of_custom_modules_to_import is not None:
        module = map(import_module, list_of_custom_modules_to_import)
        return module, list_of_custom_modules_to_import


def get_adapters(path_to_config: str = "config.yaml") -> map:
    """For the modules provided its core files are loaded"""
    module, list_of_modules = import_custom_packages(path=path_to_config)
    list_of_underlying_packages = []
    for l, module in enumerate(list(module)):
        path = module.__path__
        filelist = []
        for root, dirs, files in os.walk(path[0]):
            for file in files:
                # append the file name to the list
                filelist.append(os.path.join(root, file))
        for i in filelist:
            if i.endswith('.py') and not i.endswith("__init__.py"):
                modules = i.split(".")[0].split("/")
                modules = modules[modules.index(list_of_modules[l]):]
                modules = '.'.join(modules)
                list_of_underlying_packages.append(modules)
    list_to_import = map(import_module, list_of_underlying_packages)
    return list_to_import

