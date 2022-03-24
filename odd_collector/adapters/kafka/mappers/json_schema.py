from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup, DataSetField
from oddrn_generator import KafkaGenerator
from typing import Dict, List



def json_schema(data: dict, oddrn_generator: KafkaGenerator)->List[DataSetField]:
    pass