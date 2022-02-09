from typing import List, Literal, Optional, Union
import pydantic
from typing_extensions import Annotated


class Plugin(pydantic.BaseSettings):
    name: str
    description: Optional[str] = None
    namespace: Optional[str] = None
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_region: str


class GluePlugin(Plugin):
    type: Literal["glue"]


class DynamoDbPlugin(Plugin):
    type: Literal["dynamodb"]
    exclude_tables: Optional[List[str]] = []


class AthenaPlugin(Plugin):
    type: Literal["athena"]


class S3Plugin(Plugin):
    type: Literal["s3"]
    buckets: Optional[List[str]] = []


class QuicksightPlugin(Plugin):
    type: Literal["quicksight"]


class SagemakerPlugin(Plugin):
    type: Literal["sagemaker_featurestore"]


AvailablePlugin = Annotated[
    Union[
        GluePlugin,
        DynamoDbPlugin,
        AthenaPlugin,
        S3Plugin,
        QuicksightPlugin,
        SagemakerPlugin,
    ],
    pydantic.Field(discriminator="type"),
]
