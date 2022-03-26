from oddrn_generator import Generator
from typing import Dict, List
from itsdangerous import NoneAlgorithm
from odd_models.models import DataSetField, DataSetFieldType, Type
from .types import TYPES_MONGO_TO_ODD



def json_schema(data: dict, oddrn_generator: Generator)->List[DataSetField]:
    print("*************json schema******************")
    print(data)
    print("************json schema*******************")