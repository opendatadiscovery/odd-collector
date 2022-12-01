from typing import Optional

from .table import Table


class View(Table):
    view_definition: Optional[str]
    is_updatable: str
    is_secure: str
    view_comment: Optional[str]
