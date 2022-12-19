from typing import Dict, Type
from urllib.parse import urlparse

from funcy import lconcat, lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList

from odd_collector.domain.plugin import MlflowPlugin

from .client import Client, MlflowClientBase
from .generator import MlFlowGenerator
from .mappers import map_experiment, map_model, map_model_version, map_run


class Adapter(AbstractAdapter):
    def __init__(self, config: MlflowPlugin, client: Type[MlflowClientBase] = Client):
        self.config = config
        self.client = client(config)
        self.generator = MlFlowGenerator(
            host_settings=urlparse(config.tracking_uri).netloc
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        experiment_entities: Dict[str, DataEntity] = {}
        runs_entities: Dict[str, DataEntity] = {}
        models_entities: Dict[str, DataEntity] = {}
        model_versions_entities: Dict[str, DataEntity] = {}

        for experiment in self.client.get_experiments():
            self.generator.set_oddrn_paths(experiments=experiment.name)

            runs: Dict[str, DataEntity] = dict(
                map_run(self.generator, run) for run in experiment.runs
            )

            experiment_id, experiment_entity = map_experiment(
                self.generator,
                lpluck_attr("oddrn", runs.values()),
                experiment,
            )

            experiment_entities[experiment_id] = experiment_entity
            runs_entities.update(runs)

        for model in self.client.get_models():
            self.generator.set_oddrn_paths(models=model.name)

            _model_data_entity: DataEntity = map_model(self.generator, [], model)
            _model_versions_entities: Dict[str, DataEntity] = {}

            for model_version in model.model_versions:
                model_version_entity = map_model_version(self.generator, model_version)
                if model_version.run_id in runs_entities:
                    runs_entities[model_version.run_id].data_transformer.outputs.append(
                        model_version_entity.oddrn
                    )
                _model_data_entity.data_entity_group.entities_list.append(
                    model_version_entity.oddrn
                )

                _model_versions_entities[model_version.full_name] = model_version_entity

            model_versions_entities[model.name] = _model_data_entity
            model_versions_entities.update(_model_versions_entities)

        items = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=lconcat(
                runs_entities.values(),
                experiment_entities.values(),
                model_versions_entities.values(),
                models_entities.values(),
            ),
        )

        with open("mlflow.json", "w") as file:
            file.write(items.json())

        return items
