import logging
from typing import List

from neo4j import GraphDatabase
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Neo4jGenerator

from . import _find_all_nodes, _find_all_nodes_relations
from .mappers.nodes import map_nodes


class Adapter(AbstractAdapter):
    __connection = None

    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__oddrn_generator = Neo4jGenerator(host_settings=f"{self.__host}", databases=self.__database)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_datasets(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_datasets(self) -> List[DataEntity]:
        try:
            self.__connect()

            nodes = self.__execute(_find_all_nodes)

            relations = self.__execute(_find_all_nodes_relations)

            self.__disconnect()

            logging.info(f'Load {len(nodes)} nodes and {len(relations)} relations from Neo4j database')

            return map_nodes(self.__oddrn_generator, nodes, relations)
        except Exception as e:
            logging.error('Failed to load metadata for tables')
            logging.exception(e)
            self.__disconnect()
        return []

    def __query(self, tx, cyp: str) -> list:
        return tx.run(cyp).values()

    def __execute(self, cyp: str) -> list:
        with self.__connection.session() as session:
            return session.read_transaction(self.__query, cyp)

    def __connect(self):
        self.__connection = GraphDatabase.driver(f"bolt://{self.__host}:{self.__port}", auth=(self.__user, self.__password), encrypted=False)

    def __disconnect(self):
        if self.__connection:
            self.__connection.close()