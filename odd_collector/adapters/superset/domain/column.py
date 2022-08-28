class Column:
    def __init__(
            self,
            id: str,
            name: str,
            remote_type: str = None,
    ):
        self.id = id
        self.name = name
        self.remote_type = remote_type

    @staticmethod
    def from_response(response):
        return Column(
            response.get("id"),
            response.get("name"),
            response.get("type"),
        )