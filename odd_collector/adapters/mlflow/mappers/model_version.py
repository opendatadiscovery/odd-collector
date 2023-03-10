from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    MetadataExtension,
)

from ..domain.model_version import ModelVersion
from ..generator import MlFlowGenerator
from .metadata import MODEL_VERSION_SCHEMA


def map_model_version(generator: MlFlowGenerator, mv: ModelVersion) -> DataEntity:
    generator.set_oddrn_paths(model_versions=mv.version)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("model_versions"),
        name=mv.full_name,
        type=DataEntityType.JOB,
        created_at=mv.created_at.isoformat(),
        owner=mv.user_id,
        updated_at=mv.updated_at.isoformat() if mv.updated_at else None,
        metadata=[
            MetadataExtension(schema_url=MODEL_VERSION_SCHEMA, metadata=mv.metadata)
        ],
        data_transformer=DataTransformer(inputs=[], outputs=[]),
    )
