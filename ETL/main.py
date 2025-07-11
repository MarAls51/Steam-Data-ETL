import argparse
import ExtractData.ExtractSteamData as ExtractSteamData
from LoadData.LoadSteamData import Database
import TransformData.TransformSteamData as TransformData

def cron_job_fetch_games(db: Database):
    """
    Fetch all available Steam games and replace the GAMES table in the database.
    """
    games_df = ExtractSteamData.fetch_all_steam_games()
    db.replace_table(games_df, "GAMES")

def cron_job_fetch_all_sql_data(db: Database):
    """
    Export all SQL tables (GAMES, REVIEWS, USERS) to CSV files.
    """
    db.export_tables_to_csv()

def fetch_reviews_for_game(db: Database, gameid, review_limit):
    """
    Fetch reviews for a specific game by ID, transform the data, and insert into USERS and REVIEWS tables.

    Args:
        db (Database): The database connection instance.
        gameid (int): Steam App ID of the game.
        review_limit (int): Optional limit on number of reviews to fetch.
    """
    reviews_df = ExtractSteamData.fetch_game_reviews(gameid, review_limit)
    users_df, reviews_df = TransformData.transform_review_data(gameid, reviews_df)
    db.append_table(users_df, "USERS")
    db.append_table(reviews_df, "REVIEWS")

def parse_arguments():
    """
    Parse command-line arguments to determine which ETL operation to run.
    
    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Steam ETL: Fetch and update Steam game data and reviews."
    )
    parser.add_argument(
        '--update-games',
        action='store_true',
        help='Fetch and update the newest games from Steam.'
    )
    parser.add_argument(
        '--fetch-sql-data',
        action='store_true',
        help='Export all SQL tables to CSV files.'
    )
    parser.add_argument(
        '--game-id',
        type=int,
        help='Fetch reviews for a specific game by its App ID.'
    )
    parser.add_argument(
        '--review-limit',
        type=int,
        default=None,
        help='Limit the number of reviews fetched.'
    )
    return parser.parse_args()

def main():
    """
    Main entry point for the ETL script. Connects to the database and executes the selected task.
    """
    db = Database()
    if not db.connect():
        return

    args = parse_arguments()

    if args.update_games:
        cron_job_fetch_games(db)
    elif args.game_id:
        fetch_reviews_for_game(db, args.game_id, args.review_limit)
    elif args.fetch_sql_data:
        cron_job_fetch_all_sql_data(db)
    else:
        print("No action specified.")

if __name__ == "__main__":
    main()
