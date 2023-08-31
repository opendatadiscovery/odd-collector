from dataclasses import dataclass
from datetime import datetime

from odd_collector_sdk.utils.metadata import HasMetadata
from odd_collector.adapters.ckan.utils import get_metadata, get_groups


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
class Dataset(CKANObject):
    @property
    def tags(self) -> list[str]:
        return self.data["tags"]

    @property
    def resources(self) -> list[dict]:
        return self.data["resources"]

    @property
    def odd_metadata(self) -> dict:
        metadata = get_metadata(self.data, self.excluded_keys)
        transformed = get_groups(metadata)
        return transformed


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
    def odd_metadata(self) -> dict:
        return self.data["schema"]

    @property
    def is_nullable(self) -> bool:
        return False if self.odd_metadata["notnull"] else True
