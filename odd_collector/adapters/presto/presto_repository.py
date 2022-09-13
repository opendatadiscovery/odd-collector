from prestodb.dbapi import connect
from typing import List, Union
from .mappers import catalogs_to_exclude, schemas_to_exclude


class PrestoRepository:
    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__user = config.user

    @property
    def server_url(self):
        return f"{self.__host}:{self.__port}"

    @staticmethod
    def iterable_to_str(inst: Union[list, set]) -> str:
        return ', '.join(f"'{w}'" for w in inst)

    def __execute(self, query: str) -> List[list]:
        presto_conn_params = {
            "host": self.__host,
            "port": self.__port,
            "user": self.__user,
        }
        with connect(**presto_conn_params) as conn:
            cur = conn.cursor()
            cur.execute(query)
            records = cur.fetchall()
            return records

    @property
    def __columns_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, column_name, type_name
            FROM system.jdbc.columns 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    @property
    def __tables_query(self):
        return f"""
            SELECT table_cat, table_schem, table_name, table_type
            FROM system.jdbc.tables 
            WHERE table_cat NOT IN ({self.iterable_to_str(catalogs_to_exclude)})
            AND table_schem NOT IN ({self.iterable_to_str(schemas_to_exclude)})

        """

    def get_columns(self) -> List[list]:
        return self.__execute(self.__columns_query)

    def get_tables(self) -> List[list]:
        return self.__execute(self.__tables_query)
