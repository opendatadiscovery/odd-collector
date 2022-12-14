from typing import List

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    MetadataExtension,
)

from ..domain.experiment import ExperimentEntity
from ..generator import MlFlowGenerator
from .metadata import EXPERIMENT_SCHEMA


def map_experiment(
    oddrn_generator: MlFlowGenerator,
    experiment: ExperimentEntity,
    jobs_oddrn: List[str],
) -> DataEntity:
    return DataEntity(
        oddrn=experiment.get_oddrn(oddrn_generator),
        name=experiment.name,
        metadata=[
            MetadataExtension(
                schema_url=EXPERIMENT_SCHEMA, metadata=experiment.metadata
            )
        ],
        tags=None,
        type=DataEntityType.ML_EXPERIMENT,
        data_entity_group=DataEntityGroup(entities_list=jobs_oddrn),
    )
