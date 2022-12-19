from typing import List, Tuple

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    MetadataExtension,
)

from ..domain.experiment import Experiment
from ..generator import MlFlowGenerator
from .metadata import EXPERIMENT_SCHEMA


def map_experiment(
    generator: MlFlowGenerator,
    runs_oddrn: List[str],
    experiment: Experiment,
) -> Tuple[str, DataEntity]:
    """Mapping domain models to tuple of mapped Experiment with Runs

    Args:
        generator (MlFlowGenerator): instance of generator
        runs_oddrn (List[str]): list of runs oddrn
        experiment (Experiment): experiment model mapped from MlFlow

    Returns:
        Tuple[str, DataEntity] -> [experiment_id, mapped_entity]
    """
    generator.set_oddrn_paths(experiments=experiment.experiment_id)

    experiment_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("experiments"),
        name=experiment.name,
        created_at=experiment.creation_time.isoformat(),
        updated_at=experiment.last_update_time.isoformat()
        if experiment.last_update_time
        else None,
        metadata=[
            MetadataExtension(
                schema_url=EXPERIMENT_SCHEMA, metadata=experiment.metadata
            )
        ],
        type=DataEntityType.ML_EXPERIMENT,
        data_entity_group=DataEntityGroup(entities_list=runs_oddrn),
    )

    return experiment.experiment_id, experiment_entity
