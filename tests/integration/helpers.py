from funcy import filter, lfilter
from odd_models import DataEntity
from odd_models.models import DataEntityList, DataEntityType


def find_by_type(
    data_entity_list: DataEntityList, data_entity_type: DataEntityType
) -> list[DataEntity]:
    """Find data entities by type."""
    return lfilter(
        lambda data_entity: data_entity.type == data_entity_type, data_entity_list.items
    )


def find_by_name(data_entity_list: DataEntityList, name: str) -> DataEntity:
    return next(
        filter(
            lambda data_entity: data_entity.name == name,
            data_entity_list.items,
        )
    )
