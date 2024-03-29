CREATE TABLE IF NOT EXISTS videoGamesRatings (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR UNIQUE,
    average_rating DECIMAL(3, 2),
    num_users_rated INT,
    oldest_rating DATE,
    newest_rating DATE
);

