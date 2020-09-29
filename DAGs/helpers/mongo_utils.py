"""
 Mongo Operations
"""
from pymongo import (MongoClient, UpdateOne,)


class MongoOperations:
    def __init__(self):
        pass

    @staticmethod
    def establish_mongo_connection(mongo_uri, db_name):
        """
        establish MongoDB connection
        : param mongo_uri: mongo connection uri
        : return db_connection: database connection
        """
        client = MongoClient(mongo_uri)

        db_connection = client[db_name]
        return db_connection

    @staticmethod
    def construct_mongo_upsert_query(data, target_column):
        """
        construct mongo query to upsert documents
        :param data:
        :param target_column:
        :return:
        """
        # use bulk update MongoDB API to save the data
        unique_ids = [item.pop(target_column) for item in data]
        operations = [
            UpdateOne(
                {'{}'.format(target_column): unique_id},
                {'$set': data},
                upsert=True
            ) for unique_id, data in zip(unique_ids, data)
        ]

        return operations
