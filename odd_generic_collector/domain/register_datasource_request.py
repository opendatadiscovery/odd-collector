import pydantic
from typing import Optional
from typing import List


class RegisterDataSourceRequest(pydantic.BaseModel):
    name: str
    oddrn: str
    description: Optional[str] = None
    namespace: Optional[str] = None


class RegisterDataSourceRequests(pydantic.BaseModel):
    __root__: List[RegisterDataSourceRequest]
