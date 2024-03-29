import requests
import zipfile
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# URL of the ZIP file containing the JSON data
zip_file_url = "https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/5df5dd/games_ratings.zip"

# Download the ZIP file
response = requests.get(zip_file_url)
zip_file_path = "games_ratings.zip"
with open(zip_file_path, "wb") as zip_file:
    zip_file.write(response.content)

# Extract JSON data from the ZIP file
json_file_name = "Video_Games_5.json"
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extract(json_file_name)


def load_json_file(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data

# Example usage
data = load_json_file('Video_Games_5.json')

# Load JSON data

#with open(json_file_name, "r", encoding="utf-8") as json_file:
#    data = json.load(json_file)

# Connect to MongoDB
uri = "mongodb+srv://siny:siny@clusterinitiatedb.2avlfuj.mongodb.net/?retryWrites=true&w=majority&appName=ClusterInitiateDB"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

db = client.get_database('video_games')
collection = db.get_collection('videoGamesRatings')
try:
    # Insert data into MongoDB collection
    collection.insert_many(data)
    print("Data inserted successfully into MongoDB collection")

except Exception as e:
    print(e)



