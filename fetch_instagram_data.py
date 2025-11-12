import pandas as pd
import requests
import os
from datetime import datetime
import time
import logging
import numpy as np

# ----------------------------
# CONFIG
# ----------------------------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
BUCKET_NAME = "Marketing Database"
INSTAGRAM_USERNAME = "extrastaff.recruitment"
INSTAGRAM_USER_ID = "75427613659"  # From your working code

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("instagram_data_fetcher.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ----------------------------
# Supabase CSV Upload (HTTP with upsert)
# ----------------------------
def upload_csv_to_supabase(file_path, bucket_name):
    """Uploads CSV to Supabase Storage, overwriting existing files if necessary."""
    file_name = os.path.basename(file_path)
    file_path = os.path.abspath(file_path)

    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        upload_url = f"{SUPABASE_URL}/storage/v1/object/{bucket_name}/{file_name}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "text/csv",
            "x-upsert": "true"  # forces overwrite
        }

        response = requests.post(upload_url, headers=headers, data=file_bytes)

        if response.status_code in (200, 201):
            logging.info(f"✅ Successfully uploaded '{file_name}' to '{bucket_name}'.")
        else:
            logging.error(f"❌ Supabase upload failed ({response.status_code}): {response.text}")

    except Exception as e:
        logging.error(f"❌ Upload failed for {file_name}: {e}")

# ----------------------------
# Function to create Instagram post URL
# ----------------------------
def create_instagram_url(shortcode):
    return f"https://www.instagram.com/p/{shortcode}/"

# ----------------------------
# Get Instagram Follower Count
# ----------------------------
def get_follower_count():
    api_key = SCRAPINGDOG_API_KEY
    url = "https://api.scrapingdog.com/instagram/profile"
    
    params = {
        "api_key": api_key,
        "username": INSTAGRAM_USERNAME
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            followers_count = data.get('followers_count', 0)
            
            # Save follower count to CSV
            save_follower_count(followers_count)
            
            return followers_count
        else:
            logging.error(f"API request failed with status code: {response.status_code}")
            return 0
    except Exception as e:
        logging.error(f"Error fetching follower count: {str(e)}")
        return 0

# ----------------------------
# Save Follower Count to CSV
# ----------------------------
def save_follower_count(followers_count):
    try:
        # File path for follower count data
        file_path = 'follower_history.csv'
        
        # Current timestamp
        current_time = datetime.now()
        
        # Create new row
        new_row = {
            'timestamp': current_time,
            'date': current_time.date(),
            'time': current_time.time(),
            'followers_count': followers_count,
            'username': INSTAGRAM_USERNAME
        }
        
        logging.info(f"📝 New follower data: {followers_count} followers at {current_time}")
        
        # Check if file exists (it should, since it's in your repo)
        if os.path.exists(file_path):
            try:
                # Read the existing CSV file from your repo
                existing_df = pd.read_csv(file_path)
                logging.info(f"📁 Found existing file with {len(existing_df)} records")
                
                # Convert timestamp column if it exists and is string
                if 'timestamp' in existing_df.columns and existing_df['timestamp'].dtype == 'object':
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                
                # Create new dataframe for the new row
                new_df = pd.DataFrame([new_row])
                
                # Append new row to existing data
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                logging.info(f"🔄 Appended new record. Total records: {len(updated_df)}")
                
            except Exception as e:
                logging.error(f"❌ Error reading existing file: {e}")
                # If reading fails, create new dataframe with current data
                updated_df = pd.DataFrame([new_row])
        else:
            # Create new file if it doesn't exist (shouldn't happen since it's in repo)
            updated_df = pd.DataFrame([new_row])
            logging.warning("⚠️ File not found, creating new one")
        
        # Save updated data back to the same CSV file in repo
        updated_df.to_csv(file_path, index=False)
        logging.info(f"💾 Successfully updated {file_path} with new follower count")
        
        # Upload the updated file to Supabase
        upload_csv_to_supabase(file_path, BUCKET_NAME)
        
        # Also update daily summary
        save_daily_summary(updated_df)
        
        logging.info(f"✅ Successfully saved follower count: {followers_count}")
        
    except Exception as e:
        logging.error(f"❌ Error saving follower count: {str(e)}")

# ----------------------------
# Save Daily Summary
# ----------------------------
def save_daily_summary(df):
    try:
        # Convert timestamp to datetime if it's string
        if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Group by date and get latest follower count for each day
        daily_summary = df.groupby('date').agg({
            'followers_count': 'last',
            'timestamp': 'count'
        }).rename(columns={'timestamp': 'records_count'}).reset_index()
        
        # Add username
        daily_summary['username'] = INSTAGRAM_USERNAME
        
        # Save daily summary
        daily_file_path = 'daily_follower_summary.csv'
        daily_summary.to_csv(daily_file_path, index=False)
        
        # Upload daily summary to Supabase
        upload_csv_to_supabase(daily_file_path, BUCKET_NAME)
        
        logging.info("✅ Daily summary saved and uploaded successfully")
        
    except Exception as e:
        logging.error(f"❌ Error saving daily summary: {str(e)}")

# ----------------------------
# Fetch Instagram Posts (FROM YOUR WORKING CODE)
# ----------------------------
def fetch_instagram_posts():
    """Fetch Instagram posts using your working code"""
    try:
        # Delete existing ig.csv file before fetching new data
        if os.path.exists('ig.csv'):
            os.remove('ig.csv')
            logging.info("🗑️ Deleted existing ig.csv file")
        
        api_key = SCRAPINGDOG_API_KEY
        url = "https://api.scrapingdog.com/instagram/posts"
        user_id = INSTAGRAM_USER_ID

        all_posts = []
        next_token = None

        logging.info("📡 Fetching Instagram posts...")

        while True:
            params = {
                "api_key": api_key,
                "id": user_id
            }
            if next_token:
                params["next_page_token"] = next_token

            response = requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                logging.error(f"❌ Request failed: {response.status_code}")
                break

            data = response.json()
            posts_data = data.get("posts_data", [])
            all_posts.extend(posts_data)
            logging.info(f"📥 Fetched {len(posts_data)} posts in this batch")

            next_page = data.get("next_page_token", {})
            if not next_page.get("has_next_page"):
                logging.info("✅ Reached end of posts")
                break

            next_token = next_page.get("token")
            logging.info("🔄 Fetching next page...")

        logging.info(f"🎯 Total posts fetched: {len(all_posts)}")
        
        if all_posts:
            # Process and save the posts data
            process_and_save_posts(all_posts)
        else:
            logging.warning("⚠️ No posts fetched from API")
            
    except Exception as e:
        logging.error(f"❌ Error fetching Instagram posts: {str(e)}")

# ----------------------------
# Process and Save Posts Data (FROM YOUR WORKING CODE)
# ----------------------------
def process_and_save_posts(posts_data):
    """Process the fetched posts data and save to CSV"""
    try:
        # Flatten owner dictionary into separate columns
        flattened_data = []
        for item in posts_data:
            flat_item = item.copy()
            owner_info = flat_item.pop('owner', {})
            flat_item['owner_id'] = owner_info.get('id')
            flat_item['owner_username'] = owner_info.get('username')
            flattened_data.append(flat_item)

        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)
        
        # Handle likes (replace -1 with 0 as in your working code)
        df['likes'] = df['likes'].apply(lambda x: np.where(x == -1, 0, x))
        
        # Select and rename columns to match your expected format
        column_mapping = {
            'id': 'shortcode',
            'timestamp': 'timestamp',
            'likes': 'likes',
            'comments': 'comment',
            'caption': 'caption',
            'is_video': 'is_video'
        }
        
        # Keep only the columns we need and rename them
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns]
        df = df.rename(columns=column_mapping)
        
        # Ensure all required columns exist
        required_columns = ['timestamp', 'shortcode', 'likes', 'comment', 'caption', 'is_video']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Save the new data directly (no need to check for existing file since we deleted it)
        df.drop_duplicates(inplace=True)
        df.to_csv('ig.csv', index=False)
        
        # Upload to Supabase
        upload_csv_to_supabase('ig.csv', BUCKET_NAME)
        
        logging.info(f"✅ Instagram posts data saved and uploaded: {len(df)} posts")
        
        # Also create a processed version with better formatting
        create_processed_posts_data(df)
        
    except Exception as e:
        logging.error(f"❌ Error processing and saving posts: {str(e)}")

# ----------------------------
# Create Processed Posts Data
# ----------------------------
def create_processed_posts_data(df):
    """Create a processed version of posts data with better formatting"""
    try:
        if df.empty:
            logging.warning("⚠️ No data to process for posts")
            return
            
        # Create a processed version
        processed_df = df.copy()
        
        # Ensure datetime column exists
        if 'datetime' not in processed_df.columns and 'timestamp' in processed_df.columns:
            processed_df['datetime'] = pd.to_datetime(processed_df['timestamp'], unit='s')
        
        # Add Instagram URLs
        if 'shortcode' in processed_df.columns:
            processed_df['instagram_url'] = processed_df['shortcode'].apply(create_instagram_url)
        
        # Select relevant columns for the processed version
        processed_columns = ['datetime', 'shortcode', 'likes', 'comment', 'caption', 'is_video', 'instagram_url']
        available_columns = [col for col in processed_columns if col in processed_df.columns]
        
        processed_df = processed_df[available_columns]
        
        # Sort by datetime (newest first)
        if 'datetime' in processed_df.columns:
            processed_df = processed_df.sort_values('datetime', ascending=False)
        
        # Save processed data
        processed_file_path = 'instagram_posts_processed.csv'
        processed_df.to_csv(processed_file_path, index=False)
        
        # Upload to Supabase
        upload_csv_to_supabase(processed_file_path, BUCKET_NAME)
        
        logging.info(f"✅ Processed posts data created and uploaded: {len(processed_df)} posts")
        
    except Exception as e:
        logging.error(f"❌ Error creating processed posts data: {str(e)}")

# ----------------------------
# Load Follower History
# ----------------------------
def load_follower_history():
    try:
        file_path = 'follower_history.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading follower history: {str(e)}")
        return pd.DataFrame()

# ----------------------------
# Main execution function
# ----------------------------
def main():
    logging.info("🚀 Starting Instagram data fetch...")
    
    # Fetch follower count
    logging.info("📊 Fetching follower count...")
    follower_count = get_follower_count()
    logging.info(f"✅ Current follower count: {follower_count}")
    
    # Fetch Instagram posts data
    logging.info("📝 Fetching Instagram posts data...")
    fetch_instagram_posts()
    
    logging.info("🎉 Instagram data fetch completed successfully!")

if __name__ == "__main__":
    main()
