from dataclasses import dataclass, field, fields
from typing import Any, Dict, Optional

from funcy import omit, select


@dataclass
class Column:
    table_cat: str
    table_schem: str
    table_name: str
    column_name: str
    data_type: str
    type_name: Any
    column_size: Any
    buffer_length: int
    decimal_digits: Any
    num_prec_radix: Any
    nullable: Any
    remarks: Any
    column_def: Any
    sql_data_type: str
    sql_datetime_sub: str
    char_octet_length: int
    ordinal_position: int
    is_nullable: Any
    metadata: Optional[Dict] = field(default=None)

    @classmethod
    def from_response(cls, response: Dict[str, Any]):
        """
        Different ODBC drivers can return additional fields for column.py (Over 17 default fields)
        All additional fields goes to metadata
        """
        class_fields = {field.name for field in fields(cls)}

        class_params = select(lambda obj: obj[0] in class_fields, response)
        class_params["metadata"] = omit(response, class_fields)

        return cls(**class_params)
