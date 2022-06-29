import logging
import vertica_python


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

    def __query(self):
        pass

    def __execute(self):
        pass

    def __exit__(self):
        pass
