import argparse
import ExtractData.ExtractSteamData as ExtractData

def cron_job_fetch_games():
    games_df = ExtractData.fetch_all_steam_games()
    print(games_df.head())

def fetch_reviews_for_game(gameid, review_limit):
    reviews_df = ExtractData.fetch_game_reviews(gameid, review_limit)
    print(reviews_df.head())

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Steam ETL: Fetch and update Steam game data and reviews."
    )
    parser.add_argument(
        '--update-games', 
        action='store_true', 
        help='Fetch and update the newest games from Steam.'
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
    args = parse_arguments()

    if args.update_games:
        cron_job_fetch_games()
    elif args.game_id:
        fetch_reviews_for_game(args.game_id, args.review_limit)
    else:
        print("No action specified.")

if __name__ == "__main__":
    main()
