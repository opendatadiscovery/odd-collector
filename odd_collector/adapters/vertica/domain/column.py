class Column:
    def __init__(
            self,
            table_schema: str,
            table_name: str,
            is_system_table: bool,
            column_id: str,
            column_name: str,
            data_type: str,
            is_primary_key: bool,
            data_type_id: str,
            data_type_length: str,
            character_maximum_length: str,
            numeric_precision: str,
            numeric_scale: str,
            datetime_precision: str,
            interval_precision: str,
            ordinal_position: str,
            is_nullable: bool,
            column_default: str,
            column_set_using: str,
            is_identity: bool,
            is_primary_key_enabled: str,
            description: str = None,
    ):
        self.table_schema = table_schema
        self.table_name = table_name
        self.is_system_table = is_system_table
        self.column_id = column_id
        self.column_name = column_name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.data_type_id = data_type_id
        self.data_type_length = data_type_length
        self.character_maximum_length = character_maximum_length
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
        self.datetime_precision = datetime_precision
        self.interval_precision = interval_precision
        self.ordinal_position = ordinal_position
        self.is_nullable = is_nullable
        self.column_default = column_default
        self.column_set_using = column_set_using
        self.is_identity = is_identity
        self.is_primary_key_enabled = is_primary_key_enabled
        self.description = description

    @staticmethod
    def from_response(response):
        return Column(*response)
