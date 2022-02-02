import logging
from typing import Any

import pyarrow
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow.lib import Schema


class S3ParquetSchemaRetriever:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_region: str) -> None:
        self.__fs = pyarrow.fs.S3FileSystem(
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
            region=aws_region
        )

    def get_schema(self, path: str) -> Schema:
        logging.debug(f"Fetching schema for {path}")
        return self.__fetch_schema(path)

    def __fetch_schema(self, p) -> Any:
        return pq \
            .ParquetFile(source=self.__fs.open_input_file(p)) \
            .schema_arrow
