from typing import Optional, List

from pydantic import BaseModel, validator

from funcy import omit


class TableMetadata(BaseModel):
    id: str
    name_in_source: str
    parent_id: str
    name_in_destination: str

    @validator("*", pre=True)
    def lower_case_strings(cls, v):
        if isinstance(v, str):
            return v.lower()
        return v

    @property
    def metadata(self):
        excluded = ["name_in_source", "name_in_destination"]
        return omit(self.__dict__, excluded)
