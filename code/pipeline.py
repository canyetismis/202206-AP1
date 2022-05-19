import geopandas as gpd
from mongodb.content_provider import ContentProvider
from shapely.geometry import mapping
from pymongo.errors import OperationFailure
import re
import os

print("Starting the data preprocessing pipeline...\n")
print("Intialising database connection...")
contentProvider = ContentProvider("mongo_test", "geodata_prod")
print("Database connection establised!\n")
contentProvider.delete_data()


base_dir = './data'
print("Searching for the data in " + base_dir)
dataset_list = os.listdir(base_dir)
print("Datasets found:")
for i in range(len(dataset_list)):
    print(dataset_list[i])
print("\n")

# Data preprocessing related functions
def cast_to_list(polygon):
    polygon['coordinates'] = list(polygon['coordinates'])
    polygon['coordinates'][0] = list(polygon['coordinates'][0])
    for j in range(len(polygon['coordinates'][0])):
        polygon['coordinates'][0][j] = list(polygon['coordinates'][0][j]) 
    
    return polygon

def normalise_coordinates(dataframe):
    for i in range(len(dataframe)):
        shape = cast_to_list(dataframe['geometry'].loc[i])
        for j in range(len(shape['coordinates'][0])):
            shape['coordinates'][0][j][0] = shape['coordinates'][0][j][0] - 180
    
    return dataframe

def index_data():
    try:
        contentProvider.create_2dsphere_index("geometry")
    except OperationFailure as e:
        index = re.search('_id: "(.*)"', str(e))
        index = index.group(1).split('"',1)
        print("Broken value detected: " + index[0])
        contentProvider.delete_data({"_id" : index[0]})
        index_data()

print("Initialising main pipeline sequence...")
for dataset in dataset_list:
    print("Processing the dataset " + dataset)

    #read all experiment data
    shape_file_path = base_dir + '/' + dataset + '/' + dataset + ".shp"
    data = gpd.read_file(shape_file_path)

    #create an _id column for MongoDB
    data["_id"] = dataset + '_' + data.index.astype(str)

    #make _id the first column
    cols = data.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    data = data[cols]

    #Get the maximum and minimum values for latitude and longitude values
    coordinates = data['geometry'].bounds
    #transform Shapely Polygons to Python Dictionaries (Necessary for inserting data into the database)
    data['geometry'] = data['geometry'].apply(lambda x:mapping(x))

    #Check if longitude values are between 0 and 360
    for i in range(len(coordinates)):
        if coordinates['maxx'].loc[i] > 180:
            data = normalise_coordinates(data)
            break

    contentProvider.insert_data(data)

    print(dataset + " processed and inserted to the database successfully!")

print("Main pipeline sequence completed!")
print("Indexing data...")
index_data()
print("Indexing completed, bye!")
del contentProvider