from odd_models.models import DataEntity, DataEntityType

from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.models.view import View


def connect_views(views: list[View], data_entities: list[DataEntity]) -> None:
    """Connect views with dependencies"""
    view_entities: dict[str, DataEntity] = dict()
    tables_entities: dict[str, DataEntity] = dict()

    try:
        for data_entity in data_entities:
            if data_entity.type == DataEntityType.TABLE:
                tables_entities[data_entity.name] = data_entity

            if data_entity.type == DataEntityType.VIEW:
                view_entities[data_entity.name] = data_entity

        for view in views:
            for dependency_name in view.depends_on:
                if dependency_name in tables_entities:
                    add_to_inputs(
                        view_entities[view.table_name],
                        tables_entities[dependency_name].oddrn,
                    )
                elif dependency_name in view_entities:
                    add_to_inputs(
                        view_entities[view.table_name],
                        view_entities[dependency_name].oddrn,
                    )
                    add_to_outputs(
                        view_entities[dependency_name],
                        view_entities[view.table_name].oddrn,
                    )
                else:
                    logger.warning(
                        f"{view.table_name} depends on {dependency_name} but no one table with that name"
                    )
    except Exception as e:
        logger.exception(f"Could not connect views. {e}")


def add_to_inputs(data_entity: DataEntity, oddrn: str):
    inputs = data_entity.data_transformer.inputs

    if oddrn not in inputs:
        inputs.append(oddrn)


def add_to_outputs(data_entity: DataEntity, oddrn: str):
    outputs = data_entity.data_transformer.outputs

    if oddrn not in outputs:
        outputs.append(oddrn)
