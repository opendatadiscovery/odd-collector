from abc import abstractmethod
from typing import Callable, Dict, List, Union

from .mappers import catalogs_to_exclude, schemas_to_exclude
from .presto_repository_base import PrestoRepositoryBase


class LdapPropertiesError(Exception):
    def __init__(self, _property_name):
        self.message = f"LDAP requires {_property_name} as well"
        super().__init__(self.message)


class PrestoRepositoryCommon(PrestoRepositoryBase):
    @abstractmethod
    def _get_conn_params(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def _connect(self) -> Callable:
        pass

    @staticmethod
    def iterable_to_str(inst: Union[list, set]) -> str:
        return ", ".join(f"'{w}'" for w in inst)

    def _execute(self, query: str) -> List[list]:
        with self._connect()(**self._get_conn_params()) as conn:
            cur = conn.cursor()
            cur.execute(query)
            records = cur.fetchall()
            return records

    def _get_columns_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, column_name, type_name
            FROM system.jdbc.columns 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    def _get_tables_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, table_type
            FROM system.jdbc.tables 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    def get_columns(self) -> List[list]:
        return self._execute(self._get_columns_query())

    def get_tables(self) -> List[list]:
        return self._execute(self._get_tables_query())
