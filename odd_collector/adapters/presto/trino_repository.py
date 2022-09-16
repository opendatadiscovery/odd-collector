from trino.dbapi import connect
from trino.auth import BasicAuthentication
from typing import Dict, Callable
from .presto_repository_common import PrestoRepositoryCommon


class TrinoRepository(PrestoRepositoryCommon):

    def _get_conn_params(self) -> Dict[str, str]:
        base_params = self.base_params

        if self._config.password is None:
            return base_params
        else:
            base_params.update(
                {
                    "http_scheme": "https",
                    "auth": BasicAuthentication(
                        self._config.user, self._config.password
                    ),
                }
            )
            return base_params

    def _connect(self) -> Callable:
        return connect
