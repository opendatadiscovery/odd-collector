from abc import ABC, abstractmethod


from abc import ABC
from odd_models.models import DataEntityList


class AbstractAdapter(ABC):
    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def get_data_entity_list(self) -> DataEntityList:
        raise NotImplementedError()
