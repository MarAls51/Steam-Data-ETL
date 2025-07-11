import pandas as pd

def transform_unix_to_datetime(df, columns):
    """
    Convert specified Unix timestamp columns in a DataFrame to datetime format.

    Args:
        df (pd.DataFrame): The input DataFrame containing Unix timestamps.
        columns (list): List of column names to convert.

    Returns:
        pd.DataFrame: The DataFrame with converted datetime columns.
    """
    for col in columns:
        df[col] = pd.to_datetime(pd.to_numeric(df[col], errors='coerce'), unit='s', errors='coerce')
    return df

def transform_review_data(gameid, reviews_df: pd.DataFrame):
    """
    Transform raw review data by cleaning types, extracting user info, and formatting timestamps.

    Args:
        gameid (int): Steam App ID associated with the reviews.
        reviews_df (pd.DataFrame): DataFrame containing raw review data.

    Returns:
        tuple: A tuple containing two DataFrames — (users_df, review_data_df)
    """
    reviews_df['appid'] = int(gameid)
    reviews_df['weighted_vote_score'] = pd.to_numeric(reviews_df['weighted_vote_score']).round(2)

    datetime_columns = ['timestamp_created', 'timestamp_updated', 'last_played']
    reviews_df = transform_unix_to_datetime(reviews_df, datetime_columns)

    playtime_columns = ['playtime_at_review', 'playtime_forever', 'playtime_last_two_weeks']
    for col in playtime_columns:
        reviews_df[col] = pd.to_numeric(reviews_df[col], errors='coerce').fillna(0).astype(int)

    user_columns = [
        'steamid', 'num_games_owned', 'num_reviews'
    ]

    review_columns = [
        'appid', 'recommendationid', 'language', 'timestamp_created', 'timestamp_updated',
        'voted_up', 'votes_up', 'votes_funny', 'weighted_vote_score', 'comment_count',
        'steam_purchase', 'received_for_free', 'written_during_early_access',
        'primarily_steam_deck', 'steamid', 'playtime_at_review', 'playtime_forever',
        'playtime_last_two_weeks', 'last_played'
    ]

    users_df = reviews_df[user_columns]
    review_data_df = reviews_df[review_columns]

    return users_df, review_data_df