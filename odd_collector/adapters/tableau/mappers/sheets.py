from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List

import pytz
from odd_models.models import DataEntity, DataConsumer, DataEntityType
from oddrn_generator import TableauGenerator

from . import (
    TABLEAU_DATETIME_FORMAT,
    DATA_CONSUMER_SCHEMA,
    DATA_CONSUMER_EXCLUDED_KEYS,
)
from .metadata import extract_metadata


def __map_date(date: str = None) -> str:
    if not date:
        return None

    return (
        datetime.strptime(date, TABLEAU_DATETIME_FORMAT)
        .replace(tzinfo=pytz.utc)
        .isoformat()
    )


def map_sheet(
    oddrn_generator: TableauGenerator, sheets: list[dict]
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []

    for sheet in sheets:
        oddrn_generator.set_oddrn_paths(
            workbooks=sheet["workbook"]["name"], sheets=sheet["name"]
        )

        created_at = __map_date(sheet["createdAt"])
        updated_at = __map_date(sheet["updatedAt"]) or created_at

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("sheets"),
            name=sheet["name"],
            owner=sheet["workbook"].get("owner", {}).get("name"),
            metadata=extract_metadata(
                DATA_CONSUMER_SCHEMA,
                sheet,
                DATA_CONSUMER_EXCLUDED_KEYS,
            ),
            created_at=created_at,
            updated_at=updated_at,
            type=DataEntityType.DASHBOARD,
            data_consumer=DataConsumer(
                inputs=_map_datasource_fields_to_oddrns(
                    oddrn_generator, sheet.get("datasourceFields", {})
                ),
                outputs=[],
            ),
        )

        data_entities.append(data_entity)
    return data_entities


def _map_datasource_fields_to_oddrns(
    oddrn_generator: TableauGenerator, datasource_fields: Dict[str, Any]
) -> List[str]:
    oddrn_gen = deepcopy(oddrn_generator)  # do not change previous oddrn ?????
    inputs_oddrns: set = set()

    for field in datasource_fields:
        for table in field["upstreamTables"]:
            oddrn_gen.set_oddrn_paths(
                databases=table.get("database", {}).get("name", "unknown_table"),
                schemas=table.get("schema") or "unknown_schema",
                tables=table.get("name"),
            )
            inputs_oddrns.add(oddrn_gen.get_oddrn_by_path("tables"))

    return list(inputs_oddrns)
