"""
Written by Can Yetismis
Jacobs University Bremen 2022
"""

from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import ServerSelectionTimeoutError
from geopandas import GeoDataFrame
import warnings

class ContentProvider:

    def __init__(self, database: str, collection: str, uri: str = None):
        """
        Content Provider initiates a new connection and provides an interface for interracting with the database.

        :param database: The name of the database (string)
        :param collection: The collection within the database (string)
        :param uri: uri to the database, by default it is set to localhost:27017 (string)
        :return: a ContentProvider object
        """

        #assigns a default value to uri in case no user input is provided
        uri = "localhost:27017" if uri is None else uri

        self.__database = database
        self.__collection = collection
        self.__uri = uri

        try:
            self.__client = MongoClient(self.__uri, serverSelectionTimeoutMS=5000)
            self.__client.server_info() #causes Timeout Error to be raised if server is unavailable
        except:
            raise ServerSelectionTimeoutError("The server specified at " + uri + " is not available")
        
        database = self.__client[self.__database]
        self.__collection = database[self.__collection]
    
    def __del__(self):
        """
        Destructor. When called the destructor also terminates the database connection.
        """

        print("Info: Terminating database connection!")
        self.__client.close()

### QUERY RELATED METHODS
        
    def query_data(self, data: dict = None):
        """
        Queries data from the selected database and collection.
        
        :param data: A pymongo query (dict)
        :return query results as a list of dictionaries
        """
        #assigns a default value to data in case no user input is provided
        #this feature queries for all the data stored within the database
        data = {} if data is None else data

        query_cursor = self.__collection.find(data)
        query = list(query_cursor)# dumps(query_cursor)
        query_cursor.close() 
        return query
    
    def insert_data(self, data: GeoDataFrame = None):
        """
        Inserts data to the desired database and collection

        :param data: A GeoDataFrame object
        """
        
        data = GeoDataFrame() if data is None else data

        if(data.empty):
            raise Exception("Dataframe cannot be null!")

        data = data.to_dict(orient='records')
        self.__collection.insert_many(data)

    def delete_data(self):
        """
        Deletes all the data saved within a collection.
        """

        self.__collection.delete_many({})

### INDEX RELATED METHODS

    def create_2dsphere_index(self, column: str = None):
        if column is None:
            warnings.warn("Cannot create index! Please specify a column to create 2dsphere index")
        else:
            self.__collection.create_index([(column, GEOSPHERE)])