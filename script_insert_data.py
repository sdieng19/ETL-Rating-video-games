import pymongo
import psycopg2
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import json
import gzip


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



def get_latest_review_date(collection_name):
    max_unixReviewTime = collection_name.aggregate([
        {"$group": {"_id": None, "max_unixReviewTime": {"$max": "$unixReviewTime"}}}
    ])
    
    latest_review_date = {}
    for result in max_unixReviewTime:
        max_unix_review_time = result["max_unixReviewTime"]
        max_unix_review_time_date = datetime.utcfromtimestamp(max_unix_review_time)
        max_unix_review_time_str = max_unix_review_time_date.strftime('%m %d, %Y')
        latest_review_date = {
            "timestamp": max_unix_review_time,
            "string_date": max_unix_review_time_str,
            "datetime": max_unix_review_time_date
        }

    return latest_review_date



def convert_review_time(review_time_str):
    return datetime.strptime(review_time_str, "%m %d, %Y")

def get_reviews_from_mongodb(collection, latest_date):
    six_months_ago = latest_date - timedelta(days=180)
    six_months_ago_str = six_months_ago.strftime("%m %d, %Y")
    six_months_ago_dt = convert_review_time(six_months_ago_str)
    
    print("six months ago:", six_months_ago_dt)

    reviews = collection.find({
        "$expr": {
            "$gte": [
                {
                    "$dateFromString": {
                        "dateString": "$reviewTime",
                        "format": "%m %d, %Y"
                    }
                },
                six_months_ago
            ]
        }
    })
    return reviews


# Fonction pour agréger les notes et les avis pour chaque jeu vidéo
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%m %d, %Y").date()
    except ValueError:
        return None

def aggregate_reviews(reviews):
    aggregated_data = {}
    for review in reviews:
        asin = review['asin']
        overall_rating = review['overall']
        review_time = parse_date(review['reviewTime'])
        
        if asin not in aggregated_data:
            aggregated_data[asin] = {
                'total_rating': 0,
                'total_reviews': 0,
                'oldest_rating_date': review_time,
                'newest_rating_date': review_time
            }
        else:
            if review_time < aggregated_data[asin]['oldest_rating_date']:
                aggregated_data[asin]['oldest_rating_date'] = review_time
            elif review_time > aggregated_data[asin]['newest_rating_date']:
                aggregated_data[asin]['newest_rating_date'] = review_time
                
            # Ensure newest_rating_date is not less than oldest_rating_date
            if aggregated_data[asin]['newest_rating_date'] < aggregated_data[asin]['oldest_rating_date']:
                aggregated_data[asin]['newest_rating_date'] = aggregated_data[asin]['oldest_rating_date']
                
        aggregated_data[asin]['total_rating'] += overall_rating
        aggregated_data[asin]['total_reviews'] += 1
        
    return aggregated_data


'''id INT PRIMARY KEY,
    game_id INT UNIQUE,
    average_rating DECIMAL(3, 2),
    num_users_rated INT,
    oldest_rating DATE,
    newest_rating DATE'''
    
def insert_into_postgresql(conn, data):
    cursor = conn.cursor()
    for asin, stats in data.items():
        average_rating = stats['total_rating'] / stats['total_reviews']
        game_id = asin
        num_users_rated = stats['total_reviews']
        oldest_rating = stats['oldest_rating_date']
        newest_rating = stats['newest_rating_date']
        
        cursor.execute("""
            INSERT INTO videoGamesRatings (game_id, average_rating, num_users_rated, oldest_rating, newest_rating) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (game_id) DO UPDATE 
            SET average_rating = EXCLUDED.average_rating, 
                num_users_rated = EXCLUDED.num_users_rated,
                oldest_rating = EXCLUDED.oldest_rating,
                newest_rating = EXCLUDED.newest_rating
        """, (game_id, average_rating, num_users_rated, oldest_rating, newest_rating))
    conn.commit()
    cursor.close()

uri = "mongodb+srv://siny:siny@clusterinitiatedb.2avlfuj.mongodb.net/?retryWrites=true&w=majority&appName=ClusterInitiateDB"

postgresql_config = {
    'host': '3.253.145.15',
    'port': 5432,
    'username': 'postgres',
    'password': 'OX6Jv5yVk2cM',
    'database': 'postgres',
}

# Exécution du script
if __name__ == "__main__":
    # Connexion à MongoDB
    mongodb_client = connect_mongodb(uri=uri, database="video_games")

    mongodb_database = mongodb_client.get_database('video_games')
    mongodb_collection = mongodb_database.get_collection('videoGamesRatings')

    latest_review = get_latest_review_date(mongodb_collection)
    latest_date = latest_review["datetime"]

    # Récupération des avis des 6 derniers mois
    reviews = get_reviews_from_mongodb(mongodb_collection, latest_date)

    # Agrégation des notes et des avis
    aggregated_data = aggregate_reviews(reviews)
    
    # Connexion à PostgreSQL
    postgresql_conn = connect_postgresql(**postgresql_config)
    
    # Insertion des résultats dans PostgreSQL
    insert_into_postgresql(postgresql_conn, aggregated_data)
    
    # Fermeture des connexions
    mongodb_client.close()
    postgresql_conn.close()
