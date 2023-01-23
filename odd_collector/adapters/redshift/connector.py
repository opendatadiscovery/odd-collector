import contextlib
import traceback
import psycopg2

from abc import abstractmethod, ABC
from .exceptions import DBRedshiftException
from .logger import logger


class AbstractConnector(ABC):  # TODO: Create one abstract connector for all adapters
    @abstractmethod
    def connection(self):
        pass


class RedshiftConnector(AbstractConnector):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self._data_source = f"postgresql://{self.__user}:{self.__password.get_secret_value()}@{self.__host}:{self.__port}/{self.__database}?connect_timeout=10"

    @contextlib.contextmanager
    def connection(self):
        self.__connect()
        yield self.__cursor
        self.__disconnect()

    def __connect(self):
        try:
            self.__connection = psycopg2.connect(self._data_source)
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as e:
            logger.error(e)
            logger.debug(traceback.format_exc())
            raise DBRedshiftException("Database error. Troubles with connecting") from e

    def __disconnect(self) -> None:
        try:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()
        except (psycopg2.OperationalError, psycopg2.InternalError) as e:
            logger.error("Error in disconnecting from database", exc_info=True)
            raise DBRedshiftException(
                "Database error. Troubles with disconnecting"
            ) from e
