__version__ = '0.1.0'

_find_all_nodes: str = 'MATCH (n) return distinct labels(n) as labels, count(*), keys(n) order by labels'

_find_all_nodes_relations: str = 'MATCH (n)-[r]-() RETURN distinct labels(n), type(r), count(*)'