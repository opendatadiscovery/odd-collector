import contextlib
import logging
from abc import ABC, abstractmethod
from snowflake import connector


class AbstractConnector(ABC):
    @abstractmethod
    def connection(self):
        pass


class SnowflakeConnector(AbstractConnector, ABC):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__account = config.account
        self.__warehouse = config.warehouse
        self.__host = config.host
        self.__port = config.port

    @contextlib.contextmanager
    def connection(self):
        self.__connect()
        yield self.__cursor
        self.__disconnect()

    def __connect(self):
        self.__connection = connector.connect(
            user=self.__user,
            password=self.__password,
            account=self.__account,
            warehouse=self.__warehouse,
            database=self.__database
            )
        self.__cursor = self.__connection.cursor().execute(
                f"USE DATABASE {self.__database}"
            )

    def __disconnect(self) -> None:
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception as e:
            logging.exception(e)
        try:
            if self.__connection:
                self.__connection.close()

        except Exception as e:
            logging.exception(e)
