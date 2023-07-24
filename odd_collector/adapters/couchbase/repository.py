from abc import ABC, abstractmethod
from typing import Any, Dict, List

from acouchbase.cluster import AsyncCluster, Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import InternalServerFailureException
from couchbase.options import ClusterOptions

from odd_collector.adapters.couchbase.models import Collection
from odd_collector.domain.plugin import CouchbasePlugin

GET_COLLECTION_INFO_QUERY = """
    infer `{bucket}`.`{scope}`.`{collection}` 
    with {{'num_sample_values': {num_sample_values}, 'sample_size': {sample_size}}}
"""
GET_META_QUERY = """
    select  `ks`.`bucket`, 
            `ks`.`datastore_id`, 
            `ks`.`id`, 
            `ks`.`name`, 
            `ks`.`namespace`, 
            `ks`.`namespace_id`, 
            `ks`.`path`, 
            `ks`.`scope` 
    from system:keyspaces `ks` where `ks`.`bucket`='{bucket}'
"""


class AbstractRepository(ABC):
    @abstractmethod
    async def get_collections(self):
        pass


class CouchbaseCluster:
    def __init__(self, host: str, user: str, password: str):
        self.cluster_address = f"couchbases://{host}"
        self.cluster_options = ClusterOptions(PasswordAuthenticator(user, password))

    async def __aenter__(self) -> AsyncCluster:
        self.cluster = Cluster(self.cluster_address, self.cluster_options)
        await self.cluster.on_connect()
        return self.cluster

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.cluster.close()


class CouchbaseRepository(AbstractRepository):
    def __init__(self, config: CouchbasePlugin):
        self.host = config.host
        self.user = config.user
        self.password = config.password.get_secret_value()
        self.bucket = config.bucket
        self.sample_size = config.sample_size
        self.num_sample_values = config.num_sample_values

    async def get_collections(self) -> List[Collection]:
        async with CouchbaseCluster(self.host, self.user, self.password) as cluster:
            metadata = await cluster.query(
                GET_META_QUERY.format(bucket=self.bucket)
            ).execute()
            collections = []
            if metadata:
                for meta in metadata:
                    cols: dict = {}
                    query = GET_COLLECTION_INFO_QUERY.format(
                        bucket=self.bucket,
                        scope=meta["scope"],
                        collection=meta["name"],
                        sample_size=self.sample_size,
                        num_sample_values=self.num_sample_values,
                    )
                    try:
                        async for row in cluster.query(query):
                            cols = row[0].get("properties", []) if any(row) else cols
                    except InternalServerFailureException as err:
                        # No documents found for collection, unable to infer schema.
                        if err.context.first_error_code == 7014:
                            pass
                    collections.append(Collection(**meta, columns=cols))
        return collections
