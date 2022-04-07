from collections import namedtuple

_query_select_tables = '''
SELECT 
    t.TBL_ID,
    t.TBL_NAME,
    t.TBL_TYPE,
    t.DB_ID,
    d.NAME,
    t.CREATE_TIME,
    t.LAST_ACCESS_TIME,
    t.OWNER
    ,MAX(IF(s.PARAM_KEY = 'COLUMN_STATS_ACCURATE', s.PARAM_VALUE, NULL)) AS `COLUMN_STATS`
    ,MAX(IF(s.PARAM_KEY = 'bucketing_version', s.PARAM_VALUE, NULL)) AS `BUCK_VERSION`
    ,MAX(IF(s.PARAM_KEY = 'numFiles', s.PARAM_VALUE, NULL)) AS `NUMBER_FILES`
    ,MAX(IF(s.PARAM_KEY = 'numRows', s.PARAM_VALUE, NULL)) AS `NUMBER_FILES`
    ,MAX(IF(s.PARAM_KEY = 'rawDataSize', s.PARAM_VALUE, NULL)) AS `NUMBER_FILES`
    ,MAX(IF(s.PARAM_KEY = 'totalSize', s.PARAM_VALUE, NULL)) AS `NUMBER_FILES`
    ,MAX(IF(s.PARAM_KEY = 'transient_lastDdlTime', s.PARAM_VALUE, NULL)) AS `NUMBER_FILES`
FROM hive.TBLS t
LEFT JOIN hive.TABLE_PARAMS s ON t.TBL_ID = s.TBL_ID 
LEFT JOIN hive.DBS d ON t.DB_ID = d.DB_ID 
GROUP BY t.TBL_ID ORDER BY t.DB_ID, t.TBL_ID
LIMIT 2
'''

_query_select_columns = '''
SELECT 
    CD_ID AS TABLE_ID, 
    COLUMN_NAME, 
    TYPE_NAME 
FROM COLUMNS_V2 
ORDER BY CD_ID
'''

_query_select_stats = '''
SELECT 
    t.CS_ID,
    t.DB_NAME,
    t.TABLE_NAME,
    t.COLUMN_NAME, 
    t.COLUMN_TYPE, 
    t.TBL_ID,
    t.LONG_LOW_VALUE,
    t.LONG_HIGH_VALUE,
    t.DOUBLE_HIGH_VALUE,
    t.DOUBLE_LOW_VALUE,
    t.BIG_DECIMAL_LOW_VALUE,
    t.BIG_DECIMAL_HIGH_VALUE,
    t.NUM_NULLS,
    t.NUM_DISTINCTS,
    t.AVG_COL_LEN,
    t.MAX_COL_LEN,
    t.NUM_TRUES,
    t.NUM_FALSES
FROM TAB_COL_STATS t
ORDER BY t.TBL_ID, t.CS_ID
'''
_table_fields = ('table_id', 'table_name', 'table_type', 'db_id', 'db_name', 'create_time', 'last_access_time', 'owner',
                 'column_stats_accurate', 'bucketing_version', 'num_files', 'num_rows', 'raw_data_size',
                 'total_size', 'transient_lastDdlTime')
_columns_fields = ('table_id', 'column_name', 'type_name',)
_columns_stats_fields = ('cs_id', 'db_name', 'table_name', 'column_name', 'column_type', 'table_id',
                         'long_low_value', 'long_high_value', 'double_high_value', 'double_low_value',
                         'big_decimal_low_value', 'big_decimal_high_value', 'num_nulls', 'num_distincts',
                         'avg_col_len', 'max_col_len', 'num_trues', 'num_falses')

TableNamedTuple = namedtuple('TableNamedTuple', _table_fields)
ColumnsNamedTuple = namedtuple('ColumnsNamedTuple', _columns_fields)
StatsNamedTuple = namedtuple('StatsNamedTuple', _columns_stats_fields)

_metadata_excluded_keys: set = {'table_name', 'owner', 'last_access_time', 'create_time', 'num_rows', 'db_name'}
