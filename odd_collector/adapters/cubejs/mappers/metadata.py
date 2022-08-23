from odd_models.models import MetadataExtension

from ..domain import Cube

SCHEMA_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/cubejs.json#/definitions/Cube"
)


def map_metadata(cube: Cube):
    return MetadataExtension(
        schema_url=SCHEMA_URL,
        metadata={
            "measures": ", ".join(m.name for m in cube.measures),
            "dimensions": ", ".join(d.name for d in cube.dimensions),
            "fileName": cube.file_name,
        },
    )
