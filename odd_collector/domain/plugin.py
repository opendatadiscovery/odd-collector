from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated


class Plugins(Plugin):
    host: str
    port: int
    database: str
    user: str
    password: str


class PostgreSQLPlugin(Plugins):
    type: Literal["postgresql"]


class MySQLPlugin(Plugins):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(Plugins):
    type: Literal["clickhouse"]


class RedshiftPlugin(Plugins):
    type: Literal["redshift"]


class MongoDBPlugin(Plugins):
    type: Literal["mongodb"]
    protocol: str

AvailablePlugin = Annotated[
    Union[
        PostgreSQLPlugin,
        MySQLPlugin,
        ClickhousePlugin,
        RedshiftPlugin,
        MongoDBPlugin,
    ],
    pydantic.Field(discriminator="type"),
]
