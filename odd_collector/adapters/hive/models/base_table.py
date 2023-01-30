from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class BaseTable(BaseModel):
    table_name: str
    table_database: str
    owner: str
    table_type: str
    metadata: dict[str, Any]
    columns: list[Any] = []
    primary_keys: list[str] = []
    rows_number: Optional[int] = None
    create_time: Optional[datetime] = None
