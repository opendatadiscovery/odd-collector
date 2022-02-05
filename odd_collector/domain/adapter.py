from abc import ABC, abstractmethod


from abc import ABC
from typing import Iterable
from odd_models.models import DataEntity

class AbstractAdapter(ABC):
    @abstractmethod
    def get_data_entities(self) -> Iterable[DataEntity]:
        pass