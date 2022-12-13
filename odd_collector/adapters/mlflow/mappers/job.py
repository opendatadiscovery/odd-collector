from odd_models.models import DataTransformer, DataEntity, DataEntityType

from ..domain.job import Job
from ..generator import MlFlowGenerator


def map_job(
        oddrn_generator: MlFlowGenerator, job: Job
) -> DataEntity:
    return DataEntity(
        oddrn=job.get_oddrn(oddrn_generator),
        name=f'{job.run_id}',
        metadata=[],
        tags=None,
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            inputs=job.input_artifacts,
            outputs=job.output_artifacts
        )
    )
