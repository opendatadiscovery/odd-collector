from typing import List, Optional

import pydantic


class OddMetadata(pydantic.BaseModel):
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]]  = []
