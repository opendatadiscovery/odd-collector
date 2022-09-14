from abc import abstractmethod


class PrestoRepositoryBase:
    def __init__(self, config):
        self._config = config

    @property
    def server_url(self):
        return f"{self._config.host}:{self._config.port}"

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_tables(self):
        pass
