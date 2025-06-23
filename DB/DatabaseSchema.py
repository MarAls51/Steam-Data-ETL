from sqlalchemy import create_engine, text
import os

def create_tables():
    engine = None
    try:

        username = os.getenv('RDS_USERNAME')
        password = os.getenv('RDS_PASSWORD')
        host = os.getenv('RDS_HOST')
        port = os.getenv('RDS_PORT', 5432)
        database = os.getenv('RDS_DATABASE')

        url = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

        engine = create_engine(url)
        with engine.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS USERS (
                    steamid BIGINT PRIMARY KEY,
                    num_games_owned INT NOT NULL,
                    num_reviews INT NOT NULL
                );
            """))

            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS GAMES (
                    appid INT PRIMARY KEY,
                    name TEXT NOT NULL
                );
            """))

            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS REVIEWS (
                    recommendationid BIGINT PRIMARY KEY,
                    steamid BIGINT NOT NULL,
                    appid INT NOT NULL,
                    language VARCHAR(50),
                    timestamp_created TIMESTAMP,
                    timestamp_updated TIMESTAMP,
                    voted_up BOOLEAN,
                    votes_up INT,
                    votes_funny INT,
                    weighted_vote_score NUMERIC(5,2),
                    comment_count INT,
                    steam_purchase BOOLEAN,
                    received_for_free BOOLEAN,
                    written_during_early_access BOOLEAN,
                    primarily_steam_deck BOOLEAN,
                    playtime_at_review INT,
                    playtime_forever INT,
                    playtime_last_two_weeks INT,
                    last_played TIMESTAMP,
                    FOREIGN KEY (steamid) REFERENCES users(steamid) ON DELETE CASCADE,
                    FOREIGN KEY (appid) REFERENCES games(appid) ON DELETE CASCADE
                );
            """))

            connection.commit()
        print("Schema successfully created")
    except Exception as e:
        print(f"Error creating tables: {e}")
        engine = None

if __name__ == '__main__':
    create_tables()
