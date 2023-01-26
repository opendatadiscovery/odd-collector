from .entity import Connectable
from typing import Optional


class Pipe(Connectable):
    name: str
    definition: str
    stage_url: Optional[str]
    stage_type: str
