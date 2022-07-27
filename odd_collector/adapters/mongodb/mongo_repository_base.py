from abc import ABC, abstractmethod


class MongoRepositoryBase(ABC):
    @abstractmethod
    def retrieve_schemas(self):
        pass
