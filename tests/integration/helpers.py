from funcy import lfilter
from odd_models.models import DataEntityList, DataEntityType


def find_by_type(
    data_entity_list: DataEntityList, data_entity_type: DataEntityType
) -> list:
    """Find data entities by type."""
    pred = lambda data_entity: data_entity.type == data_entity_type

    return lfilter(pred, data_entity_list.items)
