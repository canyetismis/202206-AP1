from mongodb.content_provider import ContentProvider
import geopandas as gpd
from shapely.geometry import mapping
import json 

test = ContentProvider("mongo_test", "geodata")
print(test)

### Read and Insert data
test.delete_data()

data = gpd.read_file('search_ctx.geojson')
data = data.rename(columns={'id':'_id'})
data['geometry'] = data['geometry'].apply(lambda x:mapping(x))
test.insert_data(data)

### Query trials
query = test.query_data()

response = query.to_dict()
response = json.dumps(response)

print(response)
del test