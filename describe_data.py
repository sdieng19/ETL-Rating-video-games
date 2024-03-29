import pymongo
import psycopg2
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
import gzip
import math

# Connect to MongoDB database
def connect_mongodb(uri, database):
    client = MongoClient(uri, server_api=ServerApi('1'))
    return client

# Connect to PostgreSQL database
def connect_postgresql(host, port, username, password, database):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )
    return conn


# Fonction pour récupérer les avis des 6 derniers mois depuis MongoDB
def get_reviews_from_mongodb(db, collection_name, cutoff_date):
    collection = db[collection_name]
    six_months_ago = datetime.now() - timedelta(days=180)
    reviews = collection.find({"reviewTime": {"$gte": six_months_ago}})
    return reviews


postgres_connection = connect_postgresql(host="54.246.58.216", port="5432", username="postgres", password="Pf8wB5KQtsmz", database="postgres")
print(f"Connection : {postgres_connection}")


uri = "mongodb+srv://siny:siny@clusterinitiatedb.2avlfuj.mongodb.net/?retryWrites=true&w=majority&appName=ClusterInitiateDB"
mongodb_client = connect_mongodb(uri=uri, database="video_games")

mongodb_database = mongodb_client.get_database('video_games')
mongodb_collection = mongodb_database.get_collection('videoGamesRatings')


cursor = mongodb_collection.find().limit(2)

# Affichage des documents et de leurs types
for document in cursor:
    print(document)
    print("Types:")
    for key, value in document.items():
        print(f"{key}: {type(value)}")
    print()


# Obtention des statistiques de la base de données
database_stats = mongodb_database.command("collstats", "videoGamesRatings")

# Taille de la collection en octets
collection_size_bytes = database_stats['size']

# Conversion de la taille en mégaoctets pour une meilleure lisibilité
collection_size_mb = collection_size_bytes / (1024 * 1024)

print(f"Taille de la collection: {collection_size_mb:.2f} MB")



# Récupération de tous les documents dans la collection
cursor = mongodb_collection.find()

# Variable pour garder une trace des NaN
nan_found = False

# Parcours de chaque document
for document in cursor:
    # Parcours de chaque champ du document
    for key, value in document.items():
        # Vérification si la valeur est NaN
        if isinstance(value, float) and math.isnan(value):
            print(f"NaN found in document {_id} in field {key}")
            nan_found = True

if not nan_found:
    print("No NaN values found in the collection.")



