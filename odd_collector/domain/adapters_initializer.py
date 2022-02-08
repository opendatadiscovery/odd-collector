import os
import logging

from types import ModuleType
from typing import List
from typing import Tuple
from importlib import import_module

from .plugin import Plugin
from .adapters_folder_meta import AdapterFolderMetadata
from .adapter import AbstractAdapter


def file_path_to_module_path(original_file_path: str) -> str:
    without_file_extension = original_file_path.replace(".py", "")
    module_path = without_file_extension.replace("/", ".")
    return module_path


class AdaptersInitializer:
    def __init__(self, metadata: AdapterFolderMetadata, plugins: List[Plugin]):
        self.metadata = metadata
        self.plugins = plugins

    def _import_odd_package_modules(self, package: ModuleType) -> List[ModuleType]:
        package_name = package.__name__
        package_path = package_name.replace(".", "/")

        package_modules = [
            file_path_to_module_path(os.path.join(root, file))
            for root, _, files in os.walk(package_path)
            for file in files
            if file.endswith(".py") and not file.endswith("__init__.py")
        ]

        imported = [import_module(module_path) for module_path in package_modules]

        return imported

    def _load_packages(self):
        package_with_plugin_config: List[Tuple[ModuleType, Plugin]] = []
        plugins_loaded_package = {}
        adapters_root_package = self.metadata.root_package

        for plugin in self.plugins:
            package_path = f"{adapters_root_package}.{plugin.type}"
            if package_path not in plugins_loaded_package:
                imported_package = import_module(package_path)
                self._import_odd_package_modules(imported_package)

                plugins_loaded_package[package_path] = imported_package
            else:
                logging.warning(f"package {package_path} has been already imported")

            package = plugins_loaded_package[package_path]
            package_with_plugin_config.append((package, plugin))

        return package_with_plugin_config

    def __call__(
        self,
    ) -> List[AbstractAdapter]:
        return [
            package.adapter.Adapter(plugin_config)
            for package, plugin_config in self._load_packages()
        ]
