from functools import singledispatch
from typing import List, Union

from ..domain import Card, Dashboard, Collection
from odd_models.models import MetadataExtension

prefix = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/metabase.json#/definitions"


@singledispatch
def get_schema(entity) -> str:
    ...


@get_schema.register
def _(entity: Card):
    return prefix + "/MetabaseCardExtension"


@get_schema.register
def _(entity: Dashboard):
    return prefix + "/MetabaseDashboardExtension"


@get_schema.register
def _(entity: Collection):
    return prefix + "/MetabaseCollectionExtension"


def get_metadata(entity: Union[Card, Dashboard, Collection]) -> List[MetadataExtension]:
    return [MetadataExtension(schema_url=get_schema(entity), metadata=entity.metadata)]
