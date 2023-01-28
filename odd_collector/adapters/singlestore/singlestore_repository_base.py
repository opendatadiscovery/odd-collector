from abc import ABC, abstractmethod


class SingleStoreRepositoryBase(ABC):
    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_columns(self):
        pass
