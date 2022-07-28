import logging
import pymongo

from pymongo import MongoClient

from .db_exception import DBException
from .mongo_repository_base import MongoRepositoryBase

MAX_NUMBER_OF_ITERATION = 10


class MongoRepository(MongoRepositoryBase):
    def __init__(self, config):
        self.__protocol = config.protocol
        self.__host = config.host
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password

    def retrieve_schemas(self):
        """
        This function is used to collect the schemas of a MongoDB,
        it will go return one schema for each collection. For each
        collection, the schema returned is dictionary contains the
        combination of all the types used across the first N document.
        """
        try:
            with MongoClient(
                    f"{self.__protocol}://{self.__user}:{self.__password}@{self.__host}"
            ) as mongo_client:
                connection = mongo_client[self.__database]
                collections = connection.list_collection_names()
                schemas = []
                for collection_name in collections:
                    collection = connection[collection_name]
                    schema = {
                        "title": collection_name,
                        "row_number": collection.estimated_document_count(),
                    }
                    try:
                        creation_date = (
                            collection.find({})
                            .sort("_id", 1)
                            .limit(1)
                            .next()["_id"]
                            .generation_time
                        )
                        modification_date = (
                            collection.find({})
                            .sort("_id", -1)
                            .limit(1)
                            .next()["_id"]
                            .generation_time
                        )
                    except:
                        logging.warn(
                            f"no _id field of ObjectID type in {collection_name} collection"
                        )
                        creation_date = None
                        modification_date = None

                    metadata = {
                        "index.v"
                        + str(i["v"])
                        + "."
                        + i["name"]: str([key for key, _ in i["key"].items()])
                        for i in collection.list_indexes()
                    }

                    results = collection.find({}).limit(MAX_NUMBER_OF_ITERATION)
                    merged_dict = {}
                    for i in results:
                        merged_dict = merged_dict | i

                    schema["metadata"] = metadata
                    schema["creation_date"] = creation_date
                    schema["modification_date"] = modification_date
                    schema["data"] = merged_dict

                    schemas.append(schema)
                return schemas
        except pymongo.errors.PyMongoError as mongo_exception:
            logging.error(f"MongoDB Error: {mongo_exception.args[0]}")
            raise DBException("Database error")
