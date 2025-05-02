import pandas as pd
import requests
import ExtractData.Pagination as Pagination
import re

def clean_data(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df = df.map(lambda x: re.sub(r"[^A-Za-z0-9\s\-\.,!?]", "", str(x)) if pd.notna(x) else x)
    df = df.apply(lambda col: col.map(lambda x: 0 if pd.isna(x) else x))
    
    df.replace("", pd.NA, inplace=True)
    df.dropna(axis="rows", inplace=True)
    df.drop_duplicates(inplace=True)
    
    return df

# Fetches all the steam games, works with my cron jobs. API returns the ID of the game as well as the name.
def fetch_all_steam_games() -> pd.DataFrame:
    response = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
    data = None
    if response.status_code == 200:
        data = response.json().get('applist', {}).get('apps', [])
    else:
        raise Exception("Failed to fetch Steam games")
    data = clean_data(data)
    return data
    
# fetches all reviews for a specific game
def fetch_game_reviews(gameid: str, review_limit: int = None) -> pd.DataFrame:
    data = CursorPagination.offset_pagination(gameid, review_limit)
    
    if data is None or len(data) == 0:
        raise Exception(f"Failed to fetch reviews for game with ID: {gameid}")
    
    data = clean_data(data)
    return data

