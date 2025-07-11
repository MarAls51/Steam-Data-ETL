import pandas as pd
import requests
import ExtractData.Pagination as Pagination
import re
import numpy as np

def clean_data(data: list, row_na_threshold: float = 0.5, col_na_threshold: float = 0.8) -> pd.DataFrame:
    """
    Clean a list of dictionaries by removing invalid characters, standardizing text,
    and handling missing values and duplicates.

    Args:
        data (list): List of dictionaries (raw API data).
        row_na_threshold (float): Minimum non-null ratio for rows to keep.
        col_na_threshold (float): Maximum null ratio for columns to keep.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    df = pd.DataFrame(data)

    df = df.map(lambda x: re.sub(r"[^A-Za-z0-9\s\-\.,!?]", "", str(x)) if pd.notna(x) else x)
    df = df.map(lambda x: re.sub(r'\s+', ' ', x.strip()) if isinstance(x, str) else x)
    df = df.map(lambda x: x.lower() if isinstance(x, str) else x)
    df.replace("", pd.NA, inplace=True)
    df.dropna(axis=0, thresh=int(row_na_threshold * df.shape[1]), inplace=True)
    df.dropna(axis=1, thresh=int((1 - col_na_threshold) * df.shape[0]), inplace=True)
    df.fillna(0, inplace=True)
    df.drop_duplicates(inplace=True)

    return df

def fetch_all_steam_games() -> pd.DataFrame:
    """
    Fetch the complete list of all games available on Steam.

    Returns:
        pd.DataFrame: Cleaned DataFrame of all Steam games with app ID and name.
    
    Raises:
        Exception: If the request to Steam API fails.
    """
    response = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
    data = None
    if response.status_code == 200:
        data = response.json().get('applist', {}).get('apps', [])
    else:
        raise Exception("Failed to fetch Steam games")
    data = clean_data(data)
    return data

def fetch_game_reviews(gameid: str, review_limit: int = None) -> pd.DataFrame:
    """
    Fetch all reviews for a specific game by its App ID using offset pagination.

    Args:
        gameid (str): Steam App ID of the game.
        review_limit (int, optional): Max number of reviews to fetch.

    Returns:
        pd.DataFrame: Cleaned DataFrame of review data.
    
    Raises:
        Exception: If no reviews are returned or the fetch fails.
    """
    data = Pagination.offset_pagination(gameid, review_limit)
    
    if data is None or len(data) == 0:
        raise Exception(f"Failed to fetch reviews for game with ID: {gameid}")
    
    data = clean_data(data)
    return data
