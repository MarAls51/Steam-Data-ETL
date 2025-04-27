import requests
import time

BASE_URL = "https://store.steampowered.com/appreviews/"

def cursor_pagination(gameid: str, review_limit: int = None):
    all_reviews = []
    seen_cursor = set()
    cursor = "*"
    total_reviews = 0

    while True:
        url = f"{BASE_URL}{gameid}?json=1&num_per_page=100&cursor={cursor}"

        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}\n")
            return None

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Retrying after {retry_after} seconds.\n")
            time.sleep(retry_after)
            continue

        if response.status_code != 200:
            print(f"Error fetching reviews: {response.status_code}\n")
            return None

        try:
            data = response.json()
        except ValueError as e:
            print(f"Error parsing JSON response: {str(e)}\n")
            return None

        if not data.get('success'):
            print(f"Failed to fetch reviews: {data}\n")
            return None

        reviews = data.get('reviews', [])

        if not reviews:
            print("No reviews found. Stopping pagination.\n")
            break

        for review in reviews:
            review.pop('review', "")
            author_info = review.pop('author', {}) 
            for key, value in author_info.items():
                    review[key] = value

            all_reviews.append(review)

        total_reviews += len(reviews)

        if review_limit and total_reviews >= review_limit:
            print(f"Reached review limit of {review_limit}. Stopping.\n")
            break

        cursor = data.get('cursor')

        if cursor in seen_cursor:
            break
            
        seen_cursor.add(cursor)
        
        if not cursor:
            print("No more pages available. Stopping.\n")
            break

        print(all_reviews)
    
    return all_reviews
