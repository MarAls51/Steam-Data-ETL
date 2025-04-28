import pandas as pd

def transform_review_data(gameid, reviews_df: pd.DataFrame):


    reviews_df['appid'] = int(gameid)
    reviews_df['weighted_vote_score'] = pd.to_numeric(reviews_df['weighted_vote_score'])
    reviews_df['weighted_vote_score'] = reviews_df['weighted_vote_score'].round(2)

    user_columns = [
        'steamid', 'num_games_owned', 'num_reviews'
    ]

    review_columns = [
        'appid','recommendationid', 'language', 'timestamp_created', 'timestamp_updated',
        'voted_up', 'votes_up', 'votes_funny', 'weighted_vote_score', 'comment_count',
        'steam_purchase', 'received_for_free', 'written_during_early_access',
        'primarily_steam_deck', 'steamid', 'playtime_at_review', 'playtime_forever', 'playtime_last_two_weeks', 'last_played'
    ]

    users_df = reviews_df[user_columns]
    review_data_df = reviews_df[review_columns]

    return users_df, review_data_df

