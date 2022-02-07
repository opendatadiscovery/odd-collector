from typing import List, Dict, Any

from odd_models.models import DataSetField, DataSetFieldType

TYPES_QUICKSIGHT_TO_ODD = {
    'INTEGER': 'TYPE_INTEGER',
    'DECIMAL': 'TYPE_NUMBER',
    'STRING': 'TYPE_STRING',
    'DATETIME': 'TYPE_DATETIME'
}


def __map_column(dataset_oddrn: str,
                 type_parsed: str,
                 column_name: str = None,
                 column_description: str = None,
                 is_key: bool = None,
                 is_value: bool = None
                 ) -> List[DataSetField]:
    result = []
    name = column_name if column_name is not None \
        else type_parsed

    dsf = DataSetField(
        name=name,
        oddrn=f'{dataset_oddrn}/columns/{name}',
        type=DataSetFieldType(
            type=TYPES_QUICKSIGHT_TO_ODD[type_parsed],
            logical_type=type_parsed,
            is_nullable=True
        ),
        is_key=bool(is_key),
        is_value=bool(is_value),
        owner=None,
        metadata=[],
        default_value=None,
        description=column_description
    )

    result.append(dsf)
    return result


def map_column(raw_column_data: Dict[str, Any],
               dataset_oddrn: str) -> List[DataSetField]:
    return __map_column(dataset_oddrn=dataset_oddrn,
                        column_name=raw_column_data['Name'],
                        column_description=raw_column_data.get('Description'),
                        type_parsed=raw_column_data['Type'])
