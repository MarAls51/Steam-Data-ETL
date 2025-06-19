from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def run_queries():
    load_dotenv()
    username = os.getenv('RDS_USERNAME')
    password = os.getenv('RDS_PASSWORD')
    host = os.getenv('RDS_HOST')
    port = os.getenv('RDS_PORT', 5432)
    database = os.getenv('RDS_DATABASE')

    url = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(url)

    with engine.connect() as connection:

        print("\n--- USERS ---")
        result = connection.execute(text("SELECT * FROM users"))
        for row in result:
            print(row)

        print("\n--- REVIEWS WITH GAME NAMES ---")
        result = connection.execute(text("""
            SELECT r.recommendationid, u.steamid, g.name, r.voted_up, r.weighted_vote_score
            FROM reviews r
            JOIN users u ON r.steamid = u.steamid
            JOIN games g ON r.appid = g.appid
        """))
        for row in result:
            print(row)

        print("\n--- TOP 5 GAMES BY REVIEW COUNT ---")
        result = connection.execute(text("""
            SELECT g.name, COUNT(*) AS review_count
            FROM reviews r
            JOIN games g ON r.appid = g.appid
            GROUP BY g.name
            ORDER BY review_count DESC
            LIMIT 5
        """))
        for row in result:
            print(row)

        print("\n--- AVERAGE PLAYTIME PER GAME ---")
        result = connection.execute(text("""
            SELECT g.name, AVG(r.playtime_forever) AS avg_playtime
            FROM reviews r
            JOIN games g ON r.appid = g.appid
            GROUP BY g.name
        """))
        for row in result:
            print(row)

        print("\n--- USERS WITH >10 REVIEWS ---")
        result = connection.execute(text("""
            SELECT u.steamid, COUNT(r.recommendationid) AS review_count
            FROM users u
            JOIN reviews r ON u.steamid = r.steamid
            GROUP BY u.steamid
            HAVING COUNT(r.recommendationid) > 10
        """))
        for row in result:
            print(row)

        connection.execute(text("DELETE FROM reviews WHERE appid = :appid"), {"appid": 100})
        connection.commit()
        print("\nDeleted reviews for appid=100")

        print("\n--- TOP GAMES BY TOTAL UPVOTES ---")
        result = connection.execute(text("""
            SELECT g.name, SUM(r.votes_up) AS total_upvotes
            FROM reviews r
            JOIN games g ON r.appid = g.appid
            GROUP BY g.name
            ORDER BY total_upvotes DESC
            LIMIT 5
        """))
        for row in result:
            print(row)

        print("\n--- TOP 5 USERS BY TOTAL PLAYTIME ---")
        result = connection.execute(text("""
            SELECT u.steamid, SUM(r.playtime_forever) AS total_playtime
            FROM users u
            JOIN reviews r ON u.steamid = r.steamid
            GROUP BY u.steamid
            ORDER BY total_playtime DESC
            LIMIT 5
        """))
        for row in result:
            print(row)

        print("\n--- GAMES WITH MIXED REVIEWS ---")
        result = connection.execute(text("""
            SELECT g.name,
                   SUM(r.votes_up) AS upvotes,
                   SUM(r.votes_funny + r.votes_up) AS total_votes,
                   ROUND(SUM(r.votes_up) / NULLIF(SUM(r.votes_funny + r.votes_up)::FLOAT, 0) * 100, 2) AS upvote_ratio
            FROM reviews r
            JOIN games g ON r.appid = g.appid
            GROUP BY g.name
            HAVING ROUND(SUM(r.votes_up) / NULLIF(SUM(r.votes_funny + r.votes_up)::FLOAT, 0) * 100, 2)
                    BETWEEN 40 AND 60
            ORDER BY upvote_ratio
        """))
        for row in result:
            print(row)

if __name__ == '__main__':
    try:
        run_queries()
        print("\nQueries executed successfully.")
    except Exception as e:
        print(f"Error: {e}")
