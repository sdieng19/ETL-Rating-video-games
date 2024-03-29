# ETL-Rating-video-games
Calculer la liste des jeux les mieux notés et les plus appréciés de la communauté sur les derniers jours.

# Airflow ETL Ratings DAG

This Airflow DAG (Directed Acyclic Graph) is designed to handle the extraction, transformation, and loading (ETL) of video game ratings data from MongoDB to PostgreSQL.

## Prerequisites

Before running this DAG, ensure you have the following:

- Airflow installed and configured
- MongoDB instance with the appropriate data
- PostgreSQL instance to store the transformed data
- Python libraries pymongo and psycopg2 installed

## DAG Description

This DAG consists of the following tasks:

1. **get_reviews_from_mongodb**: Extracts video game reviews data from MongoDB.
2. **aggregate_reviews**: Aggregates the reviews data to calculate total ratings, total reviews, oldest rating date, and newest rating date for each game and get the 15 best rated video games.
3. **insert_into_postgresql**: Loads the aggregated data into PostgreSQL.

## Configuration

Ensure to configure the MongoDB and PostgreSQL connections properly in the DAG file:

- **MongoDB Configuration**:
  - URI: MongoDB connection string
  - Database Name: Name of the MongoDB database
  - Collection Name: Name of the MongoDB collection containing the video game ratings data

- **PostgreSQL Configuration**:
  - Host: PostgreSQL host address
  - Port: PostgreSQL port
  - Username: PostgreSQL username
  - Password: PostgreSQL password
  - Database Name: Name of the PostgreSQL database

## Task Dependencies

The tasks in this DAG have the following dependencies:

- `get_reviews_from_mongodb` >> `aggregate_reviews`
- `aggregate_reviews` >> `insert_into_postgresql`

This ensures that the data extraction, transformation, and loading process is executed sequentially.

## Execution

This DAG is scheduled to run daily at midnight (`schedule_interval="0 0 * * *"`). You can adjust the schedule interval according to your requirements.

## Notes

- The `parse_date` function is used to convert date strings to datetime objects.
- The `get_reviews_from_mongodb_task` function retrieves reviews from MongoDB and applies a time filter to get reviews from the last 10 years.
- The `aggregate_reviews_task` function aggregates reviews data to calculate total ratings, total reviews, oldest rating date, and newest rating date for each game.
- The `insert_into_postgresql_task` function inserts the aggregated data into PostgreSQL. It updates existing records based on game_id if conflicts occur.

Ensure to review and update the MongoDB and PostgreSQL configurations, as well as any other parameters, to fit your specific environment and requirements.
