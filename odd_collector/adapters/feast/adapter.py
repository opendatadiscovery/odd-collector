import logging
from typing import Dict, List

from feast import FeatureStore, Entity
from odd_models.models import DataEntity, DataEntityList
from odd_collector_sdk.domain.adapter import AbstractAdapter
from oddrn_generator import FeastGenerator

from .mappers.datasets import FeatureViewMapper


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__feature_store = FeatureStore(repo_path=config.repo_path)
        self.__oddrn_generator = FeastGenerator(host_settings=config.host)

    def get_data_source_oddrn(self):
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        items = self.get_feature_views()
        logging.debug(f"DataEntity list: {items}")
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=items,
        )

    def __get_entities(self) -> Dict[str, Entity]:
        return {entity.name: entity for entity in self.__feature_store.list_entities()}

    def get_feature_services(self):
        return self.__feature_store.list_feature_services()

    def get_feature_views(self) -> List[DataEntity]:
        return [
            FeatureViewMapper(
                raw_feature_view,
                raw_entities_data_dict=self.__get_entities(),
                oddrn_generator=self.__oddrn_generator,
            ).get_feature_view()
            for raw_feature_view in self.__feature_store.list_feature_views()
        ]
