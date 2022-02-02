from typing import List, Literal, Optional, Union
import pydantic
from typing_extensions import Annotated


class Plugin(pydantic.BaseSettings):
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_region: str


class GluePlugin(Plugin):
    type: Literal["odd_glue_adapter"]


class DynamoDbPlugin(Plugin):
    type: Literal["odd_dynamodb_adapter"]
    exclude_tables: Optional[List[str]] = []


class AthenaPlugin(Plugin):
    type: Literal["odd_athena_adapter"]


class S3Plugin(Plugin):
    type: Literal["odd_s3_adapter"]
    buckets: Optional[List[str]] = []


AvailablePlugin = Annotated[Union[GluePlugin, DynamoDbPlugin, AthenaPlugin, S3Plugin], pydantic.Field(discriminator='type')]
