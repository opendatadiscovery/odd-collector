from abc import ABC, abstractmethod


from abc import ABC
from odd_models.models import DataEntityList


class AbstractAdapter(ABC):
    @abstractmethod
    def get_data_entity_list(self) -> DataEntityList:
        pass
