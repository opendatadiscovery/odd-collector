from typing import List

from .domain.column import Column
from .domain.table import Table
from .vertica_connector import VerticaConnector
from .vertica_repository_base import VerticaRepositoryBase


class VerticaRepository(VerticaRepositoryBase):
    def __init__(self, config):
        self.__vertica_connector = VerticaConnector(config)

    def get_tables(self) -> List[Table]:
        response = self.__execute(self.table_metadata_query)
        tables = [Table(*table) for table in response]
        return tables

    def get_columns(self) -> List[Column]:
        response = self.__execute(self.column_metadata_query)
        columns = [Column(*column) for column in response]
        return columns

    def __execute(self, query: str) -> List[tuple]:
        return self.__vertica_connector.execute(query)

    @property
    def column_metadata_query(self):
        return """
        SELECT      
            c.table_schema, 
            c.table_name,     
            c.is_system_table, 
            c.column_id,      
            c.column_name,  
            SPLIT_PART(c.data_type, '(', 1) as data_type, 
            CASE
                WHEN pk.constraint_id IS NULL
                THEN FALSE
                ELSE TRUE
            END AS is_primary_key,
            c.data_type_id, 
            c.data_type_length, 
            c.character_maximum_length, 
            c.numeric_precision, 
            c.numeric_scale, 
            c.datetime_precision, 
            c.interval_precision, 
            c.ordinal_position, 
            c.is_nullable, 
            c.column_default, 
            c.column_set_using, 
            c.is_identity,
            pk.is_enabled as is_primary_key_enabled,
            cm.comment as description
        FROM v_catalog.columns c
        LEFT JOIN v_catalog.comments cm
        ON c.table_id = cm.object_id AND c.column_name = cm.child_object
        LEFT JOIN v_catalog.primary_keys pk
        ON c.table_schema = pk.table_schema 
        AND c.table_name = pk.table_name
        AND c.column_name = pk.column_name
        ORDER BY 
            table_schema,
            table_name;
        """

    # TODO: clarify calculation method of table_rows
    #   https://dataedo.com/kb/query/vertica/list-of-tables-by-the-number-of-rows
    #   https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/Monitoring/Vertica/UsingSystemTables.htm
    @property
    def table_metadata_query(self):
        return """
            WITH at as (
                SELECT *
                FROM v_catalog.all_tables t
                WHERE t.table_type <> 'SYSTEM TABLE'),
            row_count as (
                SELECT DISTINCT ANCHOR_TABLE_ID as table_id, row_count FROM V_MONITOR.COLUMN_STORAGE 
                )
            SELECT
                at.schema_name,
                at.table_name,
                at.table_type,
                at.remarks as description,
                at.table_id,         
                (CASE at.table_type 
                    WHEN 'VIEW' THEN v.owner_name
                    WHEN 'TABLE' THEN t.owner_name
                END) as owner_name,
                (CASE at.table_type 
                    WHEN 'VIEW' THEN v.create_time
                    WHEN 'TABLE' THEN t.create_time
                END) as create_time,
                ISNULL(rc.row_count, 0) as table_rows,
                t.is_temp_table,
                t.is_system_table,
                view_definition,
                v.is_system_view
            FROM at 
            LEFT JOIN v_catalog.tables t
                ON at.table_id = t.table_id
            LEFT JOIN v_catalog.views v
                ON at.table_id = v.table_id
            LEFT JOIN row_count rc 
                ON at.table_id = rc.table_id;
        """
