import contextlib
from abc import ABC, abstractmethod

import psycopg2


class AbstractConnector(ABC):  # TODO: Create one abstract connector for all adapters
    @abstractmethod
    def connection(self):
        pass


class RedshiftConnector(AbstractConnector):
    def __init__(self, config) -> None:
        self._data_source = f"postgresql://{config.host}:{config.port}/{config.database}?user={config.user}&password={config.password.get_secret_value()}&connect_timeout={config.connection_timeout}"

    @contextlib.contextmanager
    def connection(self):
        with psycopg2.connect(self._data_source) as connection:
            with connection.cursor() as cursor:
                yield cursor
