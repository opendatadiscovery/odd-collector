from typing import Dict, Any

from odd_models.models import DataEntity, DataEntityType, DataTransformer, DataTransformerRun, Status
from oddrn_generator import GlueGenerator

from . import metadata_extractor

GLUE_JOB_STATUSES = {
    'STARTING': Status.UNKNOWN,
    'RUNNING': Status.UNKNOWN,
    'STOPPING': Status.UNKNOWN,
    'STOPPED': Status.ABORTED,
    'SUCCEEDED': Status.SUCCESS,
    'FAILED': Status.FAILED,
    'ERROR': Status.FAILED,
    'TIMEOUT': Status.FAILED,
}


def map_glue_job(raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]) -> DataEntity:
    oddrn_generator: GlueGenerator = mapper_args['oddrn_generator']
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path('jobs', raw_job_data['Name']),
        name=raw_job_data['Name'],
        owner=None,
        description=raw_job_data.get('Description', None),
        updated_at=raw_job_data['LastModifiedOn'].isoformat(),
        created_at=raw_job_data['CreatedOn'].isoformat(),
        metadata=[metadata_extractor.extract_transformer_metadata(raw_job_data)],
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            source_code_url=raw_job_data['Command']['ScriptLocation'],
            sql=None,
            inputs=[],
            outputs=[],
        )
    )


def map_glue_job_run(raw_job_run_data: Dict[str, Any], mapper_args: Dict[str, Any]) -> DataEntity:
    status = GLUE_JOB_STATUSES.get(raw_job_run_data['JobRunState'], Status.UNKNOWN)
    oddrn_generator: GlueGenerator = mapper_args['oddrn_generator']
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("runs", raw_job_run_data['Id']),
        name=raw_job_run_data['Id'],
        type=DataEntityType.JOB_RUN,
        owner=mapper_args['transformer_owner'],
        metadata=[metadata_extractor.extract_transformer_run_metadata(raw_job_run_data)],
        data_transformer_run=DataTransformerRun(
            start_time=raw_job_run_data['StartedOn'].isoformat(),
            end_time=raw_job_run_data['CompletedOn'].isoformat(),
            transformer_oddrn=oddrn_generator.get_oddrn_by_path('jobs'),
            status_reason=raw_job_run_data['ErrorMessage'] if status == 'Fail' else None,
            status=status
        )
    )
