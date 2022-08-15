from datetime import datetime
from functools import partial
from typing import List, Optional

import pytz
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataConsumer, DataEntity, DataEntityType
from oddrn_generator import TableauGenerator

from ..domain.sheet import Sheet
from . import DATA_CONSUMER_EXCLUDED_KEYS, DATA_CONSUMER_SCHEMA, TABLEAU_DATETIME_FORMAT
from .metadata import extract_metadata


def __map_date(date: str = None) -> Optional[str]:
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


def map_sheet(oddrn_generator, sheet: Sheet, tables: List[DataEntity]) -> DataEntity:
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
            created_at=__map_date(sheet.created),
            updated_at=__map_date(sheet.updated),
            type=DataEntityType.DASHBOARD,
            data_consumer=DataConsumer(inputs=[de.oddrn for de in tables]),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping sheet {sheet.name} failed") from e


def map_sheets(
    oddrn_generator: TableauGenerator,
    sheets: List[Sheet],
    tables: List[DataEntity],
) -> List[DataEntity]:
    return [map_sheet(oddrn_generator, sheet, tables) for sheet in sheets]
