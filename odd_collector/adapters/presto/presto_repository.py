from prestodb.dbapi import connect
from typing import List


class PrestoRepository:
    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__user = config.user

    @property
    def server_url(self):
        return f"{self.__host}:{self.__port}"

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
        return """
            SELECT *
            FROM system.jdbc.columns 
            WHERE table_cat != 'system' 
            AND table_schem NOT IN ('information_schema', 'sys')

        """

    def get_columns(self) -> List[list]:
        return self.__execute(self.__columns_query)
