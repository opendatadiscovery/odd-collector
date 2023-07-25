from funcy import partial
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata

get_table_metadata = partial(
    extract_metadata, datasource="postgres", definition=DefinitionType.DATASET
)
get_column_metadata = partial(
    extract_metadata, datasource="postgres", definition=DefinitionType.DATASET
)
