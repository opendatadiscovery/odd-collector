import logging
from typing import Any, Dict, Iterable, List

from odd_models.models import DataEntity, DataTransformerRun, MetadataExtension
from oddrn_generator import KubeflowGenerator

from .oddrn import generate_experiment_oddrn, generate_run_oddrn

KUBEFLOW_STORAGE_STATE = {
    "Error": "ABORTED",
    "Succeeded": "SUCCESS",
    "Failed": "FAIL",
}


def __exctract_metadata(parametres: List[Dict[str, Any]]):
    """Exctract metadata from params

    Args:
        parametres (dict[str, Any]): _description_

    Example:
    [{'name': 'data_s3_url',
        'value': None},
    {'name': 'yolov4_config_s3_url',
        'value': None},
    {'name': 'yolov4_weights_s3_url',
        'value': None}]

    Returns:
        List[MetadataExtension]: metadata
    """

    return [
        MetadataExtension(
            schema_url="https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/kubeflow.json#/definitions/Run",
            metadata={param.name: param.value for param in parametres},
        )
    ]


def map_runs(run: Iterable, oddrn_generator: KubeflowGenerator) -> DataEntity:
    """
    Raw run example:
        {  'created_at': datetime.datetime(2021, 3, 11, 17, 21, 44, tzinfo=tzutc()),
           'description': None,
           'error': None,
           'finished_at': datetime.datetime(2021, 3, 11, 17, 30, 50, tzinfo=tzutc()),
           'id': '438382df-8c03-44aa-a400-71c413473f85',
           'metrics': None,
           'name': 'test-run',
           'pipeline_spec': {'parameters': [{'name': 'data_s3_url',
                                             'value': None},
                                            {'name': 'yolov4_config_s3_url',
                                             'value': None},
                                            {'name': 'yolov4_weights_s3_url',
                                             'value': None}],
                             'pipeline_id': None,
                             'pipeline_manifest': None,
                             'pipeline_name': None,
                             'workflow_manifest': '{"kind":"Workflow","apiVersion":"argoproj.io/v1alpha1",
                                                    ....
                                                   "container":{"name":"",
                                                   "image":"394544957709.dkr.ecr.eu-central-1.amazonaws.com/training_yolov4:latest",
                                                  "command":["darknet","detector","train"]},
           'resource_references': [{'key': {'id': '86078488-0809-43d0-8177-31cec3d88302',
                                            'type': 'EXPERIMENT'},
                                    'name': 'default',
                                    'relationship': 'OWNER'},
                                   {'key': {'id': '22f26341-e96f-4758-b515-98bc9be55b40',
                                            'type': 'PIPELINE_VERSION'},
                                    'name': 'test1-2',
                                    'relationship': 'CREATOR'}],
           'scheduled_at': datetime.datetime(1970, 1, 1, 0, 0, tzinfo=tzutc()),
           'service_account': 'default-editor',
           'status': 'Failed',
           'storage_state': None}
    """
    status = KUBEFLOW_STORAGE_STATE.get(run["status"], "OTHER")

    experiment_id = run["resource_references"][0]["key"]["id"]
    run_id = run["id"]
    name = run["name"]
    parameters = run["pipeline_spec"]["parameters"]
    created_at = run["created_at"].isoformat()
    finished_at = run["finished_at"].isoformat()

    try:
        return DataEntity(
            oddrn=generate_experiment_oddrn(experiment_id, oddrn_generator),
            name=name,
            metadata=__exctract_metadata(parameters),
            data_transformer_run=DataTransformerRun(
                start_time=created_at,
                end_time=finished_at,
                transformer_oddrn=generate_run_oddrn(
                    experiment_id, run_id, oddrn_generator
                ),
                status=status,
            ),
        )

    except (TypeError, KeyError, ValueError):
        logging.warning(
            "Problems with DataEntity JSON serialization. " "Returning: {}."
        )
        return {}
