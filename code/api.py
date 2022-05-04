from fastapi import FastAPI
from typing import Optional
from mongodb.content_provider import ContentProvider


api = FastAPI()
contentProvider = ContentProvider("mongo_test", "geodata")

@api.get("/query")
def query(
    request: Optional[str] = None,
    ):
    
    request = {} if request is None else {}

    query = contentProvider.query_data(request)
    # FastAPI automatically converts lists and dictionaries to JSON objects
    return query
