import contextlib
import psycopg2

from abc import abstractmethod, ABC


class AbstractConnector(ABC):  # TODO: Create one abstract connector for all adapters
    @abstractmethod
    def connection(self):
        pass


class RedshiftConnector(AbstractConnector):
    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__connection_timeout = config.connection_timeout
        self._data_source = f"postgresql://{self.__user}:{self.__password.get_secret_value()}@{self.__host}:{self.__port}/{self.__database}?connect_timeout={self.__connection_timeout}"

    @contextlib.contextmanager
    def connection(self):
        with psycopg2.connect(self._data_source) as connection:
            with connection.cursor() as cursor:
                yield cursor
