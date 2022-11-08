from typing import Optional

from pydantic import BaseModel


class Details(BaseModel):
    port: Optional[int]
    host: Optional[str]
    dbname: Optional[str]


class Database(BaseModel):
    engine: str
    id: int
    details: Details
