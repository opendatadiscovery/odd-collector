from abc import abstractmethod


class PrestoRepositoryBase:
    def __init__(self, config):
        self._config = config

        self.base_params = {
            "host": self._config.host,
            "port": self._config.port,
            "user": self._config.user,
        }

    @property
    def server_url(self):
        return f"{self._config.host}:{self._config.port}"

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_tables(self):
        pass
