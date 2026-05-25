import requests
import pandas as pd 
import json
from datetime import datetime
import s3fs, boto3


def run_x_etl():
    # 1. Setup RapidAPI connection details
    url = "https://twitter-x-api.p.rapidapi.com/api/user/tweets"
    
    creds = boto3.Session().get_credentials().get_frozen_credentials()

    querystring = {
        "user_id": "69008563",  
        "count": "40"
    }
    
    headers = {
        "x-rapidapi-key": "c1b85edf4fmshddaddbad03fe3c5p158360jsn44479bf370e2", 
        "x-rapidapi-host": "twitter-x-api.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    try:
        print("Fetching timeline data from RapidAPI...")
        response = requests.get(url, headers=headers, params=querystring)
        
        if response.status_code != 200:
            print(f"API Error ({response.status_code}): {response.text}")
            return
            
        data = response.json()
        
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return

    # 2. Parse and Refine the Data Safely
    tweet_list = []
    
    # FIX: Point directly to the 'data' array based on log snippet
    tweets = data.get('data', [])

    if not tweets:
        print("No tweets found in the response payload. Check response structure:")
        print(json.dumps(data, indent=2)[:500]) 
        return

    for tweet in tweets:
        # Pull text safely handling both 'full_text' or standard 'text' keys
        tweet_text = tweet.get("full_text") or tweet.get("text", "")
        
        refined_tweet = {
            "user": "F1",
            "text": tweet_text,
            "favorite_count": tweet.get("favorite_count", 0),
            "retweet_count": tweet.get("retweet_count", 0),
            "created_at": tweet.get("created_at", "")
        }
        
        tweet_list.append(refined_tweet)

    # 3. Transform to DataFrame and Load to CSV
    if tweet_list:
        df = pd.DataFrame(tweet_list)
        df.to_csv('s3://x-tweet-airflow-bucket/x_tweets.csv', storage_options={"key": creds.access_key,"secret": creds.secret_key,"token": creds.token})
        print(f"Successfully processed {len(df)} tweets and saved to 'refined_tweets.csv'.")
    else:
        print("No refined data to save.")