from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any

from odd_collector_sdk.utils.metadata import HasMetadata
from odd_collector.adapters.ckan.utils import get_metadata


@dataclass
class CKANObject(HasMetadata):
    data: dict
    excluded_keys = ["name", "description", "tags", "created"]

    @property
    def name(self) -> str:
        return self.data["name"]

    @property
    def description(self) -> str:
        return self.data["description"]

    @property
    def created_at(self) -> datetime:
        return datetime.strptime(self.data["created"], "%Y-%m-%dT%H:%M:%S.%f")

    @property
    def odd_metadata(self) -> dict:
        return get_metadata(self.data, self.excluded_keys)


@dataclass
class Organization(CKANObject):
    @property
    def id(self) -> str:
        return self.data["id"]

    @property
    def tags(self) -> list[str]:
        return self.data["tags"]


@dataclass
class Group(CKANObject):
    @property
    def datasets(self) -> list[dict]:
        return [dataset for dataset in self.data["packages"]]


@dataclass
class Resource(CKANObject):
    @property
    def id(self) -> str:
        return self.data["id"]


@dataclass
class ResourceField(HasMetadata):
    data: dict

    @property
    def name(self) -> str:
        return self.data["id"]

    @property
    def type(self) -> str:
        return self.data["type"]

    @property
    def odd_metadata(self) -> dict[str, Any]:
        return self.data["schema"]

    @property
    def is_nullable(self) -> bool:
        return False if self.odd_metadata["notnull"] else True


@dataclass
class Dataset(CKANObject):
    @property
    def tags(self) -> list[str]:
        return self.data["tags"]

    @cached_property
    def resources(self) -> list[Resource]:
        return [Resource(resource) for resource in self.data["resources"]]

    @property
    def odd_metadata(self) -> dict[str, Any]:
        return get_metadata(self.data, self.excluded_keys)

    @property
    def groups(self) -> list[str]:
        return [group["name"] for group in self.data["groups"]]
