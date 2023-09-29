from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionParams:
    id: str
    name: str
    connection_type: str
    host: str
    port: int
    service: Optional[str]

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(
            id=kwargs["id"],
            name=kwargs["name"],
            connection_type=kwargs["connectionType"],
            host=kwargs["hostName"],
            port=kwargs["port"],
            service=kwargs["service"],
        )
