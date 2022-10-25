from collections import namedtuple

from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import TarantoolGenerator

from .columns import ColumnMetadata, map_column
from .metadata import append_metadata_extension, data_set_metadata_schema_url

_space_metadata: str = "id, owner, name, engine, field_count, flags, format"
_excluded_metadata_spaces: set = {
    "_cluster",
    "_func",
    "_index",
    "_vindex",
    "_priv",
    "_vpriv",
    "_schema",
    "_sequence",
    "_sequence_data",
    "_space",
    "_vspace",
    "_user",
    "_vuser",
    "_ck_constraint",
    "_collation",
    "_vcollation",
    "_session_settings",
    "_space_sequence",
    "_fk_constraint",
    "_vinyl_deferred_delete",
    "_func_index",
    "_trigger",
    "_truncate",
    "_vsequence",
    "_vfunc",
}
MetadataNamedtuple = namedtuple("MetadataNamedtuple", _space_metadata)


def map_table(
    oddrn_generator: TarantoolGenerator, spaces: list, rows_number: dict[str, int]
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []

    for space in spaces:
        metadata: MetadataNamedtuple = MetadataNamedtuple(*space)
        space_name: str = metadata.name
        if space_name in _excluded_metadata_spaces:
            continue
        oddrn_path = "spaces"

        oddrn_generator.set_oddrn_paths(**{oddrn_path: space_name})

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
            name=space_name,
            type=DataEntityType.TABLE,
            owner=metadata.owner,
            description="",
            metadata=[],
        )
        data_entities.append(data_entity)

        append_metadata_extension(
            data_entity.metadata, data_set_metadata_schema_url, metadata
        )

        data_entity.dataset = DataSet(
            rows_number=rows_number[space_name], field_list=[]
        )

        # DatasetField
        for column in metadata.format:
            # initialize 'is_nullable' in case it doesn't exist
            if len(column) < 3:
                column["is_nullable"] = "false"
            column_metadata: ColumnMetadata = ColumnMetadata(*(column.values()))
            data_entity.dataset.field_list.append(
                map_column(
                    column_metadata, oddrn_generator, data_entity.owner, oddrn_path
                )
            )
    return data_entities
