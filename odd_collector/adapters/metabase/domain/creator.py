from pydantic import BaseModel


class Creator(BaseModel):
    email: str
    id: int
    common_name: str
