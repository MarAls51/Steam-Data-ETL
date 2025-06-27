import requests
import time
import random

BASE_URL = "https://store.steampowered.com/appreviews/"
INITIAL_BACKOFF = 1.0    
MAX_BACKOFF = 60.0      

def fetch_reviews(url, seen_review_ids, attempt=1):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        backoff = min(INITIAL_BACKOFF * 2 ** (attempt - 1), MAX_BACKOFF)
        jitter = random.uniform(0, 0.5)
        print(f"Request error: {e}. Retrying in {backoff + jitter:.2f} seconds.\n")
        time.sleep(backoff + jitter)
        return fetch_reviews(url, seen_review_ids, attempt + 1)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 0))
        if retry_after == 0:
            retry_after = min(INITIAL_BACKOFF * 2 ** (attempt - 1), MAX_BACKOFF)
        jitter = random.uniform(0, 0.5)
        print(f"Rate limited (429). Retrying after {retry_after + jitter:.2f} seconds.\n")
        time.sleep(retry_after + jitter)
        return fetch_reviews(url, seen_review_ids, attempt + 1)

    if response.status_code != 200:
        print(f"Error fetching reviews: {response.status_code}\n")
        return None, None

    try:
        data = response.json()
    except ValueError as e:
        print(f"Error parsing JSON response: {str(e)}\n")
        return None, None

    if not data.get('success'):
        print(f"Failed to fetch reviews: {data}\n")
        return None, None

    reviews = []
    for review in data.get('reviews', []):
        review_id = review.get('recommendationid')
        if review_id in seen_review_ids:
            continue
        seen_review_ids.add(review_id)

        review.pop('review', "")
        author_info = review.pop('author', {})
        for key, value in author_info.items():
            review[key] = value

        reviews.append(review)

    return reviews, data

def cursor_pagination(gameid: str, review_limit: int = None):
    all_reviews = []
    seen_review_ids = set()
    seen_cursors = set()
    cursor = "*"
    per_page = 100

    while True:
        url = f"{BASE_URL}{gameid}?json=1&num_per_page={per_page}&cursor={cursor}&filter=all&day_range=365"

        reviews, data = fetch_reviews(url, seen_review_ids)
        if reviews == "retry":
            continue
        if reviews is None:
            return None
        if not reviews:
            print("No reviews found. Stopping pagination.\n")
            break

        all_reviews.extend(reviews)

        if review_limit and len(all_reviews) >= review_limit:
            print(f"Reached review limit of {review_limit}. Stopping.\n")
            break

        cursor = data.get('cursor')
        if cursor in seen_cursors or not cursor:
            print("No more pages available. Stopping.\n")
            break

        seen_cursors.add(cursor)

    return all_reviews

def offset_pagination(gameid: str, review_limit: int = None):
    all_reviews = []
    seen_review_ids = set()
    total_reviews = 0
    per_page = 100

    while True:
        url = f"{BASE_URL}{gameid}?json=1&num_per_page={per_page}&start_offset={total_reviews}&filter=all&day_range=365"

        reviews, _ = fetch_reviews(url, seen_review_ids)
        if reviews == "retry":
            continue
        if reviews is None:
            return None
        if not reviews:
            print("No more reviews available. Stopping.\n")
            break

        all_reviews.extend(reviews)

        if review_limit and len(all_reviews) >= review_limit:
            print(f"Reached review limit of {review_limit}. Stopping.\n")
            break

        total_reviews += len(reviews)
        if len(reviews) < per_page:
            print("No more pages available. Stopping.\n")
            break

    return all_reviews
