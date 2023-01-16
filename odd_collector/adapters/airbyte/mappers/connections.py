from collections.abc import MutableMapping
from typing import Any
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    MetadataExtension,
)
from oddrn_generator import AirbyteGenerator
from .oddrn import generate_connection_oddrn
from ..logger import logger


def __extract_metadata(data: dict[str, Any]) -> list[MetadataExtension]:
    def _flatten_dict_gen(d, parent_key, sep):
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                yield from flatten_dict(v, new_key, sep=sep).items()
            elif isinstance(v, list) and isinstance(v[0], str):
                v = ", ".join(item for item in v)
                yield new_key, v
            else:
                yield new_key, v

    def flatten_dict(d: MutableMapping, parent_key: str = "", sep: str = "."):
        return dict(_flatten_dict_gen(d, parent_key, sep))

    data.pop("syncCatalog", None)
    metadata = flatten_dict(data)

    return [
        MetadataExtension(
            schema_url="https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/kubeflow.json#/definitions/Pipeline",
            metadata=metadata,
        )
    ]


def map_connection(
    conn_data: tuple,
    oddrn_gen: AirbyteGenerator,
) -> DataEntity:
    """
    Mapping of connection metadata retrieved from Airbyte API
    Example data:
    {
            "connectionId": "9e34b2ab-0602-45e5-9e2e-18eca64c56a7",
            "name": "MySQL <> Postgres",
            "namespaceDefinition": "source",
            "namespaceFormat": "${SOURCE_NAMESPACE}",
            "prefix": "",
            "sourceId": "95fd1226-e40b-4fb0-8e1b-390ccbe6cd51",
            "destinationId": "d9026433-c1f2-4656-8770-9e2d6d4e5ccf",
            "operationIds": [
                "12a101fb-a5e1-4728-a827-6f889c4868a6"
            ],
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "clients",
                            "jsonSchema": {
                                "type": "object",
                                "properties": {
                                    "id": {
                                        "type": "number",
                                        "airbyte_type": "integer"
                                    },
                                    "name": {
                                        "type": "string"
                                    },
                                    "status": {
                                        "type": "string"
                                    },
                                    "logo_url": {
                                        "type": "string"
                                    },
                                    "information": {
                                        "type": "string"
                                    },
                                    "foundation_date": {
                                        "type": "string",
                                        "format": "date-time",
                                        "airbyte_type": "timestamp_with_timezone"
                                    }
                                }
                            },
                            "supportedSyncModes": [
                                "full_refresh"
                            ],
                            "defaultCursorField": [],
                            "sourceDefinedPrimaryKey": [],
                            "namespace": "test_db"
                        },
                        "config": {
                            "syncMode": "full_refresh",
                            "cursorField": [],
                            "destinationSyncMode": "overwrite",
                            "primaryKey": [],
                            "aliasName": "clients",
                            "selected": true
                        }
                    }
                ]
            },
            "schedule": {
                "units": 1,
                "timeUnit": "hours"
            },
            "scheduleType": "basic",
            "scheduleData": {
                "basicSchedule": {
                    "timeUnit": "hours",
                    "units": 1
                }
            },
            "status": "active",
            "sourceCatalogId": "b524ea8e-ec73-4bba-b5a5-ba162e5e23f5"
        }
    """
    connection_meta, inputs, outputs = conn_data
    conn_id = connection_meta.get("connectionId")
    name = connection_meta.get("name")

    try:
        return DataEntity(
            oddrn=generate_connection_oddrn(conn_id, oddrn_gen),
            name=name,
            type=DataEntityType.JOB,
            owner=None,
            updated_at=None,
            created_at=None,
            metadata=__extract_metadata(connection_meta),
            data_transformer=DataTransformer(
                description=None,
                source_code_url=None,
                sql=None,
                inputs=inputs,
                outputs=outputs,
                subtype="DATATRANSFORMER_JOB",
            ),
        )
    except (TypeError, KeyError, ValueError):
        logger.warning("Problems with DataEntity JSON serialization. " "Returning: {}.")
        return {}
