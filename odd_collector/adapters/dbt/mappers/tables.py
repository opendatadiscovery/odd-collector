from copy import deepcopy
from datetime import datetime

import pytz
from odd_models.models import DataEntity, DataSet, DataTransformer, DataEntityType
from oddrn_generator import DbtGenerator

from . import (
    _DATETIME_FORMAT,
    _data_set_metadata_schema_url,
    _data_set_metadata_excluded_keys,
)
from .columns import map_column
from .metadata import _append_metadata_extension
from .types import TABLE_TYPES_SQL_TO_ODD


def map_table(
    oddrn_generator: DbtGenerator, tables: dict, manifest: dict
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []

    for key, table in tables.items():

        table_catalog: str = table["metadata"]["database"]
        table_schema: str = table["metadata"]["schema"]
        table_name: str = table["metadata"]["name"]

        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            table["metadata"]["type"], DataEntityType.UNKNOWN
        )
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        oddrn_generator.set_oddrn_paths(
            **{
                "databases": table_catalog,
                "schemas": table_schema,
                oddrn_path: table_name,
            }
        )

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
            name=table_name,
            owner=table["metadata"].get("owner"),
            metadata=[],
            description=table["metadata"].get("comment"),
            dataset=DataSet(
                field_list=[],
                row_numbers=int(table["stats"]["row_count"]["value"])
                if table["stats"].get("row_count", {}).get("value")
                else None,
            ),
            type=data_entity_type,
        )
        data_entities.append(data_entity)

        _append_metadata_extension(
            data_entity.metadata,
            _data_set_metadata_schema_url,
            table,
            _data_set_metadata_excluded_keys,
        )

        if table["stats"].get("last_modified", {}).get("value"):
            data_entity.updated_at = (
                datetime.strptime(
                    table["stats"]["last_modified"]["value"], _DATETIME_FORMAT
                )
                .replace(tzinfo=pytz.utc)
                .isoformat()
            )
            data_entity.created_at = data_entity.updated_at

        # DataTransformer
        if (
            table["metadata"]["type"] == "VIEW"
            and key in manifest["parent_map"]
            and key in manifest["child_map"]
        ):
            data_entity.data_transformer = DataTransformer(
                inputs=_map_models_to_oddrns(
                    oddrn_generator, manifest["parent_map"][key], manifest["nodes"]
                ),
                outputs=_map_models_to_oddrns(
                    oddrn_generator, manifest["child_map"][key], manifest["nodes"]
                ),
            )
            if key in manifest["nodes"]:
                model: dict = manifest["nodes"][key]
                if model.get("root_path") and model.get("path"):
                    data_entity.data_transformer.source_code_url = (
                        f"{model['root_path']}/{model['path']}"
                    )
                if model.get("compiled_sql"):
                    data_entity.data_transformer.sql = model["compiled_sql"]

        # DatasetField
        data_entity.dataset.field_list = [
            map_column(column, oddrn_generator, data_entity.owner, oddrn_path)
            for column in table["columns"].values()
            if table["columns"] is not None
        ]
    return data_entities


def _map_models_to_oddrns(
    oddrn_generator: DbtGenerator, model_ids: list[str], models: dict
) -> list[str]:
    oddrn_gen = deepcopy(oddrn_generator)  # do not change previous oddrn

    oddrns: list[str] = []
    for model_id in model_ids:
        if model_id in models and (model_id[:6] == "model." or model_id[:5] == "seed."):
            model: dict = models[model_id]
            oddrn_gen.set_oddrn_paths(
                databases=model["database"],
                schemas=model["schema"],
                tables=model["name"],
            )
            oddrns.append(oddrn_gen.get_oddrn_by_path("tables"))
    return oddrns
