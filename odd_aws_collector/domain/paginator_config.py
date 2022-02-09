import pydantic
from typing import Dict, Union, Callable, Any, Optional


class PaginatorConfig(pydantic.BaseModel):
    op_name: str
    parameters: Dict[str, Union[str, int]] = pydantic.Field(default_factory=dict)
    page_size: int
    payload_key: str = None
    list_fetch_key: str = None
    mapper: Optional[Callable] = None
    mapper_args: Optional[Dict[str, Any]] = None

    class Config:
        smart_union = True
