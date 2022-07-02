import logging
import vertica_python
from typing import List


class VerticaDB:
    def __init__(self, config):
        self.__connection_info = {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
            "database": config.database,
        }

    def __get_connection(self):
        try:
            self.__connection = vertica_python.connect(**self.__connection_info)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            logging.error(f"Vertica connection error: {e}")

    def __query(self, columns: str, table: str, order_by: str) -> List[tuple]:
        return self.__execute(f"select {columns} from {table} order by {order_by}")

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        return self.__cursor.fetchall()


