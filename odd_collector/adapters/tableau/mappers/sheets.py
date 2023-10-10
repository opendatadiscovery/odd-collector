from datetime import datetime
from functools import partial
from typing import Optional

import pytz
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataConsumer, DataEntity, DataEntityType

from odd_collector.adapters.tableau.logger import log_entity

from ..domain.sheet import Sheet
from . import DATA_CONSUMER_EXCLUDED_KEYS, DATA_CONSUMER_SCHEMA, TABLEAU_DATETIME_FORMAT
from .metadata import extract_metadata


def map_data(date: Optional[str] = None) -> Optional[str]:
    if not date:
        return None

    return (
        datetime.strptime(date, TABLEAU_DATETIME_FORMAT)
        .replace(tzinfo=pytz.utc)
        .isoformat()
    )


extract_metadata = partial(
    extract_metadata,
    schema_url=DATA_CONSUMER_SCHEMA,
    excluded_key=DATA_CONSUMER_EXCLUDED_KEYS,
)


@log_entity
def map_sheet(oddrn_generator, sheet: Sheet) -> DataEntity:
    """
    Args:
        oddrn_generator: Generator
        sheet: Sheet
        tables: list of mapped to DataEntity tables sheet depends on

    Returns:
        DataEntity
    """
    try:
        oddrn_generator.set_oddrn_paths(workbooks=sheet.workbook, sheets=sheet.name)

        return DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("sheets"),
            name=sheet.name,
            owner=sheet.owner,
            metadata=extract_metadata(metadata={}),
            created_at=map_data(sheet.created),
            updated_at=map_data(sheet.updated),
            type=DataEntityType.DASHBOARD,
            data_consumer=DataConsumer(inputs=[]),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping sheet {sheet.name} failed") from e
