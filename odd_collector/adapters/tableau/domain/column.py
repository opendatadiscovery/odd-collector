class Column:
    def __init__(
        self,
        id: str,
        name: str,
        is_nullable: bool,
        remote_type: str = None,
        description: str = None,
    ):
        self.id = id
        self.name = name
        self.remote_type = remote_type
        self.is_nullable = is_nullable
        self.description = description or None

    @staticmethod
    def from_response(response):
        return Column(
            response.get("id"),
            response.get("name"),
            response.get("isNullable"),
            response.get("remoteType"),
            response.get("description"),
        )
