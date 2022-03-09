import logging
import pymongo
from odd_models.models import DataEntity, DataEntityList
from pymongo import MongoClient
from typing import List, Dict
from .mappers.schemas import map_collection
from .mongo_generator import MongoGenerator
from odd_collector_sdk.domain.adapter import AbstractAdapter

MAX_NUMBER_OF_ITERATION = 10
class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        self.__protocol = config.protocol
        self.__host = config.host
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__oddrn_generator = MongoGenerator(host_settings=f"{self.__host}", databases=self.__database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        This function will convert a dictionary schema into
        an odd list of data entities
        """
        try:
            self.connect()
            schemas = self.retrive_scheams()

            return map_collection(self.__oddrn_generator, schemas, self.__database)
        except Exception as e:
            logging.error('Failed to load metadata for tables')
            logging.exception(e)
        finally:
            self.disconnect()
        return []
    
    def get_data_entity_list(self) -> DataEntityList:
        res = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
        # print(res.json())
        return res

    def retrive_scheams(self) :
        """
        This function is used to collect the schemas of a MongoDB,
        it will go return one schema for each collection. For each
        collection, the schema returned is dictionary contains the
        combination of all the types used across the first N document.
        """
        try:
            collections = self.__connection.list_collection_names()
            schemas = []
            for collection_name in collections:
                collection = self.__connection[collection_name]
                schema = {"title": collection_name, 
                        "row_number":collection.estimated_document_count()
                        }
                try:
                    creation_date = collection.find({}).sort("_id", 1).limit(1).next()['_id'].generation_time
                    modification_date = collection.find({}).sort("_id", -1).limit(1).next()['_id'].generation_time
                except:
                    logging.warn(f"no _id field of ObjectID type in {collection_name} collection")
                    creation_date = None
                    modification_date = None
                
                metadata = {}
                for i in collection.list_indexes():
                    metadata['index.v'+str(i['v'])+'.'+i['name']] = [key for key,_ in i['key'].items()]

                results = collection.find({}).limit(MAX_NUMBER_OF_ITERATION)
                merged_dict = {}
                for i in results:
                    merged_dict = merged_dict | i

                schema["metadata"]=metadata
                schema["creation_date"]=creation_date
                schema["modification_date"]=modification_date
                schema['data'] = merged_dict
                
                schemas.append(schema)
            return schemas

        except Exception as e:
            print("something wrong with the schemas!")


    def connect(self):
        try:
            self.__cluster = MongoClient(
                f"{self.__protocol}://{self.__user}:{self.__password}@{self.__host}")

            self.__connection = self.__cluster[self.__database]
        except pymongo.errors.ConnectionFailure as err:
            logging.error(err)
            raise DBException('Database error')

    def disconnect(self):
        try:
            if self.__cluster:
                self.__cluster.close()
        except Exception:
            pass


class DBException(Exception):
    pass




