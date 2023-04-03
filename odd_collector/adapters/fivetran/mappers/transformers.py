from odd_models.models import DataEntity, DataTransformer, DataEntityType
from oddrn_generator import FivetranGenerator


def map_transformers(
    generator: FivetranGenerator, inputs: list[str], outputs: list[str]
) -> DataEntity:
    transformer = DataEntity(
        oddrn=generator.get_oddrn_by_path("transformers", new_value=generator.source),
        name=generator.source,
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            inputs=inputs,
            outputs=outputs,
        ),
    )
    return transformer
