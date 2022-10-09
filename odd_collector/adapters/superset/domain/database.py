class Database:
    def __init__(
            self,
            id: int,
            database_name: str,
            backend: str,
            host: str,
            port: int
    ):
        self.port = port
        self.host = host
        self.id = id
        self.database_name = database_name
        self.backend = backend
