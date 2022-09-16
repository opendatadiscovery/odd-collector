from prestodb.dbapi import connect
from prestodb.auth import BasicAuthentication
from typing import Dict, Callable
from .presto_repository_common import PrestoRepositoryCommon, LdapPropertiesError


class PrestoRepository(PrestoRepositoryCommon):
    def _get_conn_params(self) -> Dict[str, str]:
        base_params = self.base_params

        if (self._config.principal_id is None) & (self._config.password is None):
            return base_params
        else:
            if (self._config.principal_id is not None) & (
                self._config.password is not None
            ):
                base_params.update(
                    {
                        "http_scheme": "https",
                        "auth": BasicAuthentication(
                            self._config.principal_id, self._config.password
                        ),
                    }
                )
                return base_params
            else:
                if (self._config.principal_id is not None) & (
                    self._config.password is None
                ):
                    raise LdapPropertiesError("password")
                else:
                    raise LdapPropertiesError("principal_id")

    def _connect(self) -> Callable:
        return connect
