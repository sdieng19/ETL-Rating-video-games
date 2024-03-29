import os
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
import pymongo
import psycopg2
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


DAG_NAME = os.path.basename(__file__).replace(".py", "")

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'provide_context': True
}


dag = DAG(
    DAG_NAME, 
    default_args=default_args, 
    description='DAG chargement des avis sur les jeux vidéos', 
    schedule_interval="0 0 * * *"
)





# MongoDB Connection
def connect_to_mongodb(uri, database_name, collection_name):
    client = MongoClient(uri, server_api=ServerApi('1'))
    database = client.get_database(database_name)
    collection = database.get_collection(collection_name)
    return collection

# PostgreSQL Connection
def connect_to_postgresql(host, port, username, password, database_name):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database_name
    )
    return conn


    

# Task to get reviews from MongoDB
def get_reviews_from_mongodb_task(**context):
    
    execution_date = context['execution_date']
    ten_years_ago = execution_date - timedelta(days=365 * 10) - timedelta(days=180)
    ten_years_ago_timestamp = int(ten_years_ago.timestamp())

    try:
        reviews_cursor = mongodb_collection.find({"unixReviewTime": {"$gte": ten_years_ago_timestamp}}).limit(1000)
        reviews = list(reviews_cursor)
    except Exception as e:
        print("An error occurred while querying the MongoDB collection:", e)
        return []

    # Convert ObjectId and datetime objects to serializable format
    for review in reviews:
        review['_id'] = str(review['_id'])  # Convert ObjectId to string

    return reviews

    if not reviews:  
        print("No reviews found in MongoDB .")
    else:
        print("Reviews found in MongoDB:", reviews)

    return reviews

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%m %d, %Y").date()
    except ValueError:
        return None
        
# Task to aggregate reviews
def aggregate_reviews_task(**context):
    reviews = context['task_instance'].xcom_pull(task_ids='get_reviews_from_mongodb')
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
            if aggregated_data[asin]['newest_rating_date'] < aggregated_data[asin]['oldest_rating_date']:
                aggregated_data[asin]['newest_rating_date'] = aggregated_data[asin]['oldest_rating_date']
        aggregated_data[asin]['total_rating'] += overall_rating
        aggregated_data[asin]['total_reviews'] += 1
    return aggregated_data

# Task to insert into PostgreSQL
def insert_into_postgresql_task(**context):
    aggregated_reviews = context['task_instance'].xcom_pull(task_ids='aggregate_reviews')
    cursor = postgresql_conn.cursor()
    for game_id, review_data in aggregated_reviews.items():
        average_rating = review_data['total_rating'] / review_data['total_reviews']
        num_users_rated = review_data['total_reviews']
        oldest_rating = review_data['oldest_rating_date']
        newest_rating = review_data['newest_rating_date']
        cursor.execute("""
            INSERT INTO videoGamesRatings (game_id, average_rating, num_users_rated, oldest_rating, newest_rating) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (game_id) DO UPDATE 
            SET average_rating = EXCLUDED.average_rating, 
                num_users_rated = EXCLUDED.num_users_rated,
                oldest_rating = EXCLUDED.oldest_rating,
                newest_rating = EXCLUDED.newest_rating
        """, (game_id, average_rating, num_users_rated, oldest_rating, newest_rating))
    postgresql_conn.commit()
    cursor.close()



# MongoDB and PostgreSQL configurations
mongodb_config = {
    'uri': "mongodb+srv://siny:siny@clusterinitiatedb.2avlfuj.mongodb.net/?retryWrites=true&w=majority&appName=ClusterInitiateDB",
    'database_name': 'video_games',
    'collection_name': 'videoGamesRatings'
}

postgresql_config = {
    'host': '18.201.26.175',
    'port': 5432,
    'username': 'postgres',
    'password': 'ElrpeCG0M0wg',
    'database_name': 'postgres'
}

# Connect to MongoDB and PostgreSQL
mongodb_collection = connect_to_mongodb(**mongodb_config)
postgresql_conn = connect_to_postgresql(**postgresql_config)


# Task to get reviews from MongoDB
get_reviews_from_mongodb_operator = PythonOperator(
    task_id='get_reviews_from_mongodb',
    python_callable=get_reviews_from_mongodb_task,
    provide_context=True,
    dag=dag
)

# Task to aggregate reviews
aggregate_reviews_operator = PythonOperator(
    task_id='aggregate_reviews',
    python_callable=aggregate_reviews_task,
    provide_context=True,
    dag=dag
)

# Task to insert into PostgreSQL
insert_into_postgresql_operator = PythonOperator(
    task_id='insert_into_postgresql',
    python_callable=insert_into_postgresql_task,
    provide_context=True,
    dag=dag
)

# Define task dependencies
get_reviews_from_mongodb_operator >> aggregate_reviews_operator
aggregate_reviews_operator >> insert_into_postgresql_operator

