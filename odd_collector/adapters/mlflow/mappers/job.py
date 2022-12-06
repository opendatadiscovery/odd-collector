from odd_models.models import DataEntity, DataEntityType, DataEntityGroup

from ..domain.job import Job
from ..generator import MlFlowGenerator


def map_job(
    oddrn_generator: MlFlowGenerator, job: Job
) -> DataEntity:
    return DataEntity(
        oddrn=job.get_oddrn(oddrn_generator),
        name=f'job_{job.run_id}',
        metadata=[],
        tags=None,
        type=DataEntityType.ML_EXPERIMENT,
        data_entity_group=DataEntityGroup(
            entities_list=[de.oddrn for de in job]
        )
    )
