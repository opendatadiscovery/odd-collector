from odd_models.models import DataEntity, DataEntityType, DataEntityGroup

from ..domain.experiment import ExperimentEntity
from ..generator import MlFlowGenerator


def map_experiment(
    oddrn_generator: MlFlowGenerator, experiment: ExperimentEntity
) -> DataEntity:
    return DataEntity(
        oddrn=experiment.get_oddrn(oddrn_generator),
        name=f'pipeline_{experiment.name}',
        metadata=[],
        tags=None,
        type=DataEntityType.ML_EXPERIMENT,

        data_entity_group=DataEntityGroup(
            entities_list=[de.oddrn for de in experiment]
        )
    )
