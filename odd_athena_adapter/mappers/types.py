from odd_models.models import DataEntityType

TABLE_TYPES_SQL_TO_ODD: dict[str, DataEntityType] = {
    'EXTERNAL_TABLE': DataEntityType.TABLE,
    'VIRTUAL_VIEW': DataEntityType.VIEW,
}
