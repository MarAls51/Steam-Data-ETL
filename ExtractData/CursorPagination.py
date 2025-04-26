import requests
import time
import sys

BASE_URL = "https://store.steampowered.com/appreviews/"

def cursor_pagination(gameid: str, review_limit: int = None):
    all_reviews = [] 
    cursor = None
    total_reviews = 0 

    while True:
        url = f"{BASE_URL}{gameid}?json=1&num_per_page=100"
        
        if cursor:
            url += f"&cursor={cursor}" 
        
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            sys.stderr.write(f"Request failed: {str(e)}\n")
            return None

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5)) 
            sys.stderr.write(f"Rate limited. Retrying after {retry_after} seconds.\n")
            time.sleep(retry_after)
            continue

        if response.status_code != 200:
            sys.stderr.write(f"Error fetching reviews: {response.status_code}\n")
            return None

        try:
            data = response.json()
        except ValueError as e:
            sys.stderr.write(f"Error parsing JSON response: {str(e)}\n")
            return None

        if not data.get('success'):
            sys.stderr.write(f"Failed to fetch reviews: {data}\n")
            return None

        reviews = data.get('reviews', [])
        
        if not reviews:
            sys.stderr.write("No reviews found. Stopping pagination.\n")
            break 

        all_reviews.extend(reviews) 

        total_reviews += len(reviews)

        if review_limit and total_reviews >= review_limit:
            break

        cursor = data.get('cursor', None) 
        
        if not cursor:
            sys.stderr.write("No more pages available. Stopping.\n")
            break 

    return all_reviews
