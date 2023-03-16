from pydantic import BaseModel, validator

from funcy import omit


class ColumnMetadata(BaseModel):
    id: str
    name_in_source: str
    type_in_destination: str
    is_foreign_key: bool
    is_primary_key: bool
    type_in_source: str
    parent_id: str
    name_in_destination: str

    @validator("*", pre=True)
    def lower_case_strings(cls, v):
        if isinstance(v, str):
            return v.lower()
        return v

    @property
    def metadata(self):
        excluded = [
            "name_in_source",
            "name_in_destination",
            "type_in_source",
            "type_in_destination",
        ]
        return omit(self.__dict__, excluded)
