from abc import ABC, abstractmethod
from typing import Iterable

from odd_collector.adapters.odbc.domain import BaseTable
from odd_collector.domain.plugin import OdbcPlugin


class RepositoryBase(ABC):
    def __init__(self, config: OdbcPlugin) -> None:
        self._config = config

    @abstractmethod
    def get_data(self) -> Iterable[BaseTable]:
        raise NotImplementedError
