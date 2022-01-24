import logging
from datetime import datetime
from typing import Tuple, Any, Dict, Optional

from odd_models.adapter import ODDController
from odd_models.models import DataEntityList

from .adapter import DynamoDBAdapter
from .cache import Cache


class Controller(ODDController):

    def __init__(self, adapter, data_cache: Cache):
        self.__adapter = adapter
        self.__data_cache = data_cache

    def get_data_entities(self, changed_since: Dict[str, Any] = None) -> Optional[Tuple[DataEntityList, datetime]]:
        changed_since = None

        data_entities = self.__data_cache.retrieve_data_entities(changed_since)

        if data_entities is None:
            logging.warning('DataEntities cache has never been enriched')
            return None

        return DataEntityList(
            data_source_oddrn=self.__adapter.get_data_source_oddrn(),
            items=data_entities[0]
        ), data_entities[1]
