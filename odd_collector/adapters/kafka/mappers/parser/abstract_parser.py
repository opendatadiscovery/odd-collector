from abc import ABC, abstractmethod
from typing import List

from oddrn_generator import KafkaGenerator

from .types import Field, RawSchema


class AbstractParser(ABC):
    def __init__(self, oddrn_generator: KafkaGenerator) -> None:
        self.base_oddrn = oddrn_generator.get_oddrn_by_path("topics")
        super().__init__()

    @abstractmethod
    def map_schema(
        self, schema: RawSchema, references: List[RawSchema] = None
    ) -> List[Field]:
        raise NotImplementedError
