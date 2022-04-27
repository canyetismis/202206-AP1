from fastapi import FastAPI
from mongodb.content_provider import ContentProvider
import json 

api = FastAPI()
contentProvider = ContentProvider("mongo_test", "geodata")

@api.get("/query")
def query():
    query = contentProvider.query_data()
    return query
