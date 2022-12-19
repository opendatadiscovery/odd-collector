from typing import List, Tuple

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    MetadataExtension,
)
from oddrn_generator import S3Generator

from ..domain.odd_metadata import OddMetadata
from ..domain.run import Run
from ..generator import MlFlowGenerator
from .metadata import RUN_SCHEMA


def map_run(generator: MlFlowGenerator, run: Run) -> Tuple[str, DataEntity]:
    generator.set_oddrn_paths(runs=run.run_id)
    entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("runs"),
        name=run.name,
        created_at=run.start_time.isoformat(),
        owner=run.user,
        metadata=[MetadataExtension(schema_url=RUN_SCHEMA, metadata=run.metadata)],
        tags=None,
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            inputs=map_artifact_path(run.odd_metadata.inputs),
            outputs=map_artifact_path(run.odd_metadata.outputs),
        ),
    )

    return run.run_id, entity


def map_artifact_path(artifact_paths: List[str]) -> List[str]:
    oddrns = []

    for path in artifact_paths:
        if path.startswith("s3://"):
            oddrns.append(S3Generator.from_s3_url(path).get_oddrn_by_path("keys"))

    return oddrns
