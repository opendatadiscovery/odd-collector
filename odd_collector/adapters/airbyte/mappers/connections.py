import logging
from odd_models.models import DataEntity, DataEntityType, DataTransformer
from oddrn_generator import AirbyteGenerator
from .oddrn import generate_connection_oddrn, generate_dataset_oddrn
from ..api import AirbyteApi, OddPlatformApi


def map_connection(
    connection: dict,
    oddrn_gen: AirbyteGenerator,
    airbyte_api: AirbyteApi,
    odd_api: OddPlatformApi,
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
    conn_id = connection.get("connectionId")
    name = connection.get("name")

    source_oddrns = generate_dataset_oddrn(
        is_source=True,
        connection_meta=connection,
        airbyte_api=airbyte_api,
        odd_api=odd_api,
    )
    destination_oddrns = generate_dataset_oddrn(
        is_source=False,
        connection_meta=connection,
        airbyte_api=airbyte_api,
        odd_api=odd_api,
    )
    print(source_oddrns)
    print(destination_oddrns)

    try:
        return DataEntity(
            oddrn=generate_connection_oddrn(conn_id, oddrn_gen),
            name=name,
            type=DataEntityType.JOB,
            owner=None,
            updated_at=None,
            created_at=None,
            metadata=None,
            data_transformer=DataTransformer(
                description=None,
                source_code_url=None,
                sql=None,
                inputs=source_oddrns,
                outputs=destination_oddrns,
                subtype="DATATRANSFORMER_JOB",
            ),
        )
    except (TypeError, KeyError, ValueError):
        logging.warning(
            "Problems with DataEntity JSON serialization. " "Returning: {}."
        )
        return {}
