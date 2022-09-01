datasets_nodes = [
    {'changed_by': None, 'changed_by_name': '', 'changed_by_url': '', 'changed_on_delta_humanized': 'an hour ago',
     'changed_on_utc': '2022-08-31T21:50:35.320735+0000', 'database': {'database_name': 'examples', 'id': 1},
     'datasource_type': 'table', 'default_endpoint': None, 'description': None,
     'explore_url': '/explore/?dataset_type=table&dataset_id=20', 'extra': None, 'id': 20, 'kind': 'physical',
     'owners': [], 'schema': 'public', 'sql': None, 'table_name': 'threads'},

    {'changed_by': None, 'changed_by_name': '', 'changed_by_url': '', 'changed_on_delta_humanized': 'an hour ago',
     'changed_on_utc': '2022-08-31T21:50:31.009869+0000', 'database': {'database_name': 'jj', 'id': 2},
     'datasource_type': 'table', 'default_endpoint': None, 'description': None,
     'explore_url': '/explore/?dataset_type=table&dataset_id=14', 'extra': None, 'id': 14, 'kind': 'physical',
     'owners': [], 'schema': 'public', 'sql': None, 'table_name': 'channels'},

    {'changed_by': None, 'changed_by_name': '', 'changed_by_url': '', 'changed_on_delta_humanized': 'an hour ago',
     'changed_on_utc': '2022-08-31T21:50:31.009869+0000', 'database': {'database_name': 'ppp', 'id': 7},
     'datasource_type': 'table', 'default_endpoint': None, 'description': None,
     'explore_url': '/explore/?dataset_type=table&dataset_id=14', 'extra': None, 'id': 999, 'kind': 'virtual',
     'owners': [], 'schema': 'public', 'sql': None, 'table_name': 'channels'},

    {'changed_by': None, 'changed_by_name': '', 'changed_by_url': '', 'changed_on_delta_humanized': 'an hour ago',
     'changed_on_utc': '2022-08-31T21:50:35.320735+0000', 'database': {'database_name': 'examples', 'id': 1},
     'datasource_type': 'table', 'default_endpoint': None, 'description': None,
     'explore_url': '/explore/?dataset_type=table&dataset_id=20', 'extra': None, 'id': 7, 'kind': 'physical',
     'owners': [], 'schema': 'public', 'sql': None, 'table_name': 'pppp'},
]

"""
{chart_id: {dashboard_id: dashboard_name}}
"""
nodes_with_chart_ids = {1: {10: 'dash_10'}, 3: {13: 'dash_13'}, 14: {10: 'dash_10'}}

chart_nodes = [{'datasource_id': 20, 'id': 1}, {'datasource_id': 14, 'id': 14},
               {'datasource_id': 7, 'id': 3},
               ]
