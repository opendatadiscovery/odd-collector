class Column:
    def __init__(
            self,
            id: int,
            name: str,
            remote_type: str = None,
    ):
        self.id = id
        self.name = name
        self.remote_type = remote_type
        