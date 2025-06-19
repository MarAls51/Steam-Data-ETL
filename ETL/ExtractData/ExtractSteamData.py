import pandas as pd
import requests
import ExtractData.Pagination as Pagination
import re

import pandas as pd
import re
import numpy as np

def clean_data(data: list, row_na_threshold: float = 0.5, col_na_threshold: float = 0.8) -> pd.DataFrame:
    df = pd.DataFrame(data)

    df = df.map(lambda x: re.sub(r"[^A-Za-z0-9\s\-\.,!?]", "", str(x)) if pd.notna(x) else x)
    df = df.applymap(lambda x: re.sub(r'\s+', ' ', x.strip()) if isinstance(x, str) else x)
    df = df.apply(pd.to_numeric, errors='ignore')
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df.replace("", pd.NA, inplace=True)
    df.dropna(axis=0, thresh=int(row_na_threshold * df.shape[1]), inplace=True)
    df.dropna(axis=1, thresh=int((1 - col_na_threshold) * df.shape[0]), inplace=True)
    df.fillna(0, inplace=True)
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
    data = Pagination.offset_pagination(gameid, review_limit)
    
    if data is None or len(data) == 0:
        raise Exception(f"Failed to fetch reviews for game with ID: {gameid}")
    
    data = clean_data(data)
    return data

