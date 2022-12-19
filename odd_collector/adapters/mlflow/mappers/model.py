from typing import List

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    MetadataExtension,
)

from ..domain.model import Model
from ..generator import MlFlowGenerator
from .metadata import MODEL_SCHEMA


def map_model(
    generator: MlFlowGenerator, model_versions_oddrn: List[str], model: Model
):
    return DataEntity(
        name=model.name,
        oddrn=generator.get_oddrn_by_path("models"),
        created_at=model.created_at.isoformat(),
        updated_at=model.updated_at.isoformat() if model.updated_at else None,
        metadata=[MetadataExtension(schema_url=MODEL_SCHEMA, metadata=model.metadata)],
        description=model.description,
        type=DataEntityType.JOB,
        data_entity_group=DataEntityGroup(entities_list=model_versions_oddrn),
    )
