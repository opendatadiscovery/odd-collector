from functools import partial

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata

extract_metadata = partial(extract_metadata, datasource="MSSQL")
dataset_metadata = partial(extract_metadata, definition=DefinitionType.DATASET)
column_metadata = partial(extract_metadata, definition=DefinitionType.DATASET_FIELD)
