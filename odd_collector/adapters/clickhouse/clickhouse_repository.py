import logging
from abc import ABC, abstractmethod
from typing import List


class ClickHouseRepositoryBase(ABC):
    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_columns(self):
        pass


class ClickHouseRepository(ClickHouseRepositoryBase):
    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password

    def __execute(self, query: str) -> List[tuple]:
        pass
