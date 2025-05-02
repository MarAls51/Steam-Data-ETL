# just playing around with sql alchemy. 


from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def run_queries():
    load_dotenv()
    username = os.getenv('RDS_USERNAME')
    password = os.getenv('RDS_PASSWORD')
    host = os.getenv('RDS_HOST')
    port = os.getenv('RDS_PORT', 3306)
    database = os.getenv('RDS_DATABASE')

    url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(url)

    with engine.connect() as connection:

        print("\n--- USERS ---")
        result = connection.execute(text("SELECT * FROM USERS"))
        for row in result:
            print(row)
        print("\n--- REVIEWS WITH GAME NAMES ---")
        result = connection.execute(text("""
            SELECT R.recommendationid, U.steamid, G.name, R.voted_up, R.weighted_vote_score
            FROM REVIEWS R
            JOIN USERS U ON R.steamid = U.steamid
            JOIN GAMES G ON R.appid = G.appid
        """))
        for row in result:
            print(row)

        print("\n--- TOP 5 GAMES BY REVIEW COUNT ---")
        result = connection.execute(text("""
            SELECT G.name, COUNT(*) AS review_count
            FROM REVIEWS R
            JOIN GAMES G ON R.appid = G.appid
            GROUP BY G.name
            ORDER BY review_count DESC
            LIMIT 5
        """))
        for row in result:
            print(row)

        print("\n--- AVERAGE PLAYTIME PER GAME ---")
        result = connection.execute(text("""
            SELECT G.name, AVG(R.playtime_forever) AS avg_playtime
            FROM REVIEWS R
            JOIN GAMES G ON R.appid = G.appid
            GROUP BY G.name
        """))
        for row in result:
            print(row)

        print("\n--- USERS WITH >10 REVIEWS ---")
        result = connection.execute(text("""
            SELECT U.steamid, COUNT(R.recommendationid) AS review_count
            FROM USERS U
            JOIN REVIEWS R ON U.steamid = R.steamid
            GROUP BY U.steamid
            HAVING review_count > 10
        """))
        for row in result:
            print(row)

        connection.execute(text("DELETE FROM REVIEWS WHERE appid = :appid"), {"appid": 100})
        connection.commit()
        print("\nDeleted reviews for appid=100")

        connection.commit()

        print("\n--- TOP GAMES BY TOTAL UPVOTES ---")
        result = connection.execute(text("""
            SELECT G.name, SUM(R.votes_up) AS total_upvotes
            FROM REVIEWS R
            JOIN GAMES G ON R.appid = G.appid
            GROUP BY G.name
            ORDER BY total_upvotes DESC
            LIMIT 5
        """))
        for row in result:
            print(row)

if __name__ == '__main__':
    try:
        run_queries()
        print("\nQueries executed successfully.")
    except Exception as e:
        print(f"Error: {e}")
