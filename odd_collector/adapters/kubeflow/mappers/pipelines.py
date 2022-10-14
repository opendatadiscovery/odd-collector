import logging
from typing import Any, Dict, Iterable, List

from odd_models.models import DataEntity, DataTransformer, MetadataExtension
from oddrn_generator import KubeflowGenerator

from .oddrn import generate_input_oddrn, generate_pipeline_oddrn


def __extract_metadata(data: Dict[str, Any]) -> List[MetadataExtension]:
    """Exctract metadata

    Data example:
        {
            "key": {"type": "PIPELINE", "id": "45f34f06-1461-49d4-a881-251a789dacf2"},
            "name": None,
            "relationship": "OWNER",
        }

    Returns:
        List[MetadataExtension]: metadata
    """

    metadata = {
        "key.type": data.get("key").get("type"),
        "key.id": data.get("key").get("id"),
        "name": data.get("name", None),
        "relationship": data.get("relationship", None),
    }

    return [
        MetadataExtension(
            schema_url="https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/kubeflow.json#/definitions/Pipeline",
            metadata=metadata,
        )
    ]


def map_pipelines(pipeline: Iterable, oddrn_gen: KubeflowGenerator) -> DataEntity:
    """
    Raw pipeline example:
        {
        'id': '45f34f06-1461-49d4-a881-251a789dacf2',
        'created_at': datetime.datetime(2021, 3, 11, 12, 58, 19, tzinfo=tzutc()),
        'name': '[Demo] XGBoost - Training with confusion matrix',
        'description': '[source code] A trainer that does end-to-end distributed training for XGBoost models.',
        'parameters':
            [{'name': 'output', 'value': 'gs://{{kfp-default-bucket}}'},
            {'name': 'project', 'value': '{{kfp-project-id}}'},
            {'name': 'diagnostic_mode', 'value': 'HALT_ON_ERROR'},
            {'name': 'rounds', 'value': '5'}],
        'url': None,
        'error': None,
        'default_version':
            {'id': '45f34f06-1461-49d4-a881-251a789dacf2',
            'name': '[Demo] XGBoost - Training with confusion matrix',
            'created_at': datetime.datetime(2021, 3, 11, 12, 58, 19, tzinfo=tzutc()),
            'parameters': [{'name': 'output', 'value': 'gs://{{kfp-default-bucket}}'},
                            {'name': 'project', 'value': '{{kfp-project-id}}'},
                            {'name': 'diagnostic_mode', 'value': 'HALT_ON_ERROR'},
                            {'name': 'rounds', 'value': '5'}],
            'code_source_url': None,
            'package_url': None,
            'resource_references': [{'key':
                                        {'type': 'PIPELINE',
                                        'id': '45f34f06-1461-49d4-a881-251a789dacf2'},
                                    'name': None,
                                    'relationship': 'OWNER'}]}}
    """
    pip_id = pipeline.get("id")
    name = pipeline.get("name", pip_id)
    created_at = pipeline.get("created_at").isoformat()
    resource_references = pipeline.get("default_version")["resource_references"][0]
    description = pipeline.get("description", None)
    parameters = pipeline["parameters"]
    inputs: List[Dict] = (
        [{i["name"]: generate_input_oddrn(i)} for i in parameters] if parameters else []
    )
    url = pipeline.get("url", None)

    try:
        return DataEntity(
            oddrn=generate_pipeline_oddrn(pip_id, oddrn_gen),
            name=name,
            owner=None,
            updated_at=None,
            created_at=created_at,
            metadata=__extract_metadata(resource_references),
            data_transformer=DataTransformer(
                description=description,
                source_code_url=url,
                sql=None,
                inputs=inputs,
                outputs=[],
                subtype="DATATRANSFORMER_JOB",
            ),
        )
    except (TypeError, KeyError, ValueError):
        logging.warning(
            "Problems with DataEntity JSON serialization. " "Returning: {}."
        )
        return {}
