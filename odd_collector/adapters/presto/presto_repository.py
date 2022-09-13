from prestodb.dbapi import connect
from prestodb.auth import BasicAuthentication
from typing import List, Union
from typing import Dict
from .mappers import catalogs_to_exclude, schemas_to_exclude


class LdapPropertiesError(Exception):
    def __init__(self, _property_name):
        self.message = f"LDAP requires {_property_name} as well"
        super().__init__(self.message)


class PrestoRepository:
    def __init__(self, config):
        self.__config = config

    @property
    def __conn_params(self) -> Dict[str, str]:
        base_params = {
            "host": self.__config.host,
            "port": self.__config.port,
            "user": self.__config.user,
        }
        if (self.__config.principal_id is None) & (self.__config.password is None):
            return base_params
        else:
            if (self.__config.principal_id is not None) & (
                self.__config.password is not None
            ):
                base_params.update(
                    {
                        "http_scheme": "https",
                        "auth": BasicAuthentication(
                            self.__config.principal_id, self.__config.password
                        ),
                    }
                )
                return base_params
            else:
                if (self.__config.principal_id is not None) & (
                    self.__config.password is None
                ):
                    raise LdapPropertiesError("password")
                else:
                    raise LdapPropertiesError("principal_id")

    @property
    def server_url(self):
        return f"{self.__config.host}:{self.__config.port}"

    @staticmethod
    def iterable_to_str(inst: Union[list, set]) -> str:
        return ", ".join(f"'{w}'" for w in inst)

    def __execute(self, query: str) -> List[list]:
        with connect(**self.__conn_params) as conn:
            cur = conn.cursor()
            cur.execute(query)
            records = cur.fetchall()
            return records

    def __get_columns_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, column_name, type_name
            FROM system.jdbc.columns 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    def __get_tables_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, table_type
            FROM system.jdbc.tables 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    def get_columns(self) -> List[list]:
        return self.__execute(self.__get_columns_query())

    def get_tables(self) -> List[list]:
        return self.__execute(self.__get_tables_query())
