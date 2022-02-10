from typing import List, Literal, Optional, Union
import pydantic
from typing_extensions import Annotated


class Plugin(pydantic.BaseSettings):
    name: str
    description: Optional[str] = None
    namespace: Optional[str] = None
    host: str
    port: int
    database: str
    user: str
    password: str


class PostgreSQLPlugin(Plugin):
    type: Literal["postgresql"]


class MySQLPlugin(Plugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(Plugin):
    type: Literal["clickhouse"]


AvailablePlugin = Annotated[
    Union[
        PostgreSQLPlugin,
        MySQLPlugin,
        ClickhousePlugin,
    ],
    pydantic.Field(discriminator="type"),
]
