from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def create_tables():
    engine = None
    try:
        load_dotenv()

        username = os.getenv('RDS_USERNAME')
        password = os.getenv('RDS_PASSWORD')
        host = os.getenv('RDS_HOST')
        port = os.getenv('RDS_PORT', 3306)
        database = os.getenv('RDS_DATABASE')

        url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'

        engine = create_engine(url)
        with engine.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS USERS (
                    steamid BIGINT NOT NULL,
                    num_games_owned INT NOT NULL,
                    num_reviews INT NOT NULL,
                    PRIMARY KEY (steamid)
                );
            """))

            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS GAMES (
                    gameid INT NOT NULL AUTO_INCREMENT,
                    title VARCHAR(255) NOT NULL,
                    PRIMARY KEY (gameid),
                    UNIQUE (title)
                );
            """))

            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS REVIEWS (
                    reviewid INT NOT NULL AUTO_INCREMENT,
                    steamid BIGINT NOT NULL,
                    gameid INT NOT NULL,
                    recommendationid INT,
                    language VARCHAR(50),
                    timestamp_created DATETIME,
                    timestamp_updated DATETIME,
                    voted_up BOOLEAN,
                    votes_up INT,
                    votes_funny INT,
                    weighted_vote_score DECIMAL(5,2),
                    comment_count INT,
                    steam_purchase BOOLEAN,
                    received_for_free BOOLEAN,
                    written_during_early_access BOOLEAN,
                    primarily_steam_deck BOOLEAN,
                    playtime_at_review INT,
                    playtime_forever INT,
                    playtime_last_two_weeks INT,
                    last_played DATETIME,
                    PRIMARY KEY (reviewid),
                    FOREIGN KEY (steamid) REFERENCES USERS(steamid) ON DELETE CASCADE,
                    FOREIGN KEY (gameid) REFERENCES GAMES(gameid) ON DELETE CASCADE
                );
            """))

            connection.commit()
            connection.close()
        print("Schema successfully Created")
    except Exception as e:
        print(f"Error creating tables: {e}")
        engine = None

if __name__ == '__main__':
    create_tables()
