# fetch_instagram_data.py
import os
import requests
import csv
from datetime import datetime

# ✅ NO dotenv needed if secrets are from GitHub Actions
# from dotenv import load_dotenv
# load_dotenv()

# ----------- CONFIG -----------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
BUCKET_NAME         = "Marketing Database"
INSTAGRAM_USERNAME  = "extrastaff.recruitment"
CSV_FILE_PATH       = "followers_history.csv"

# ----------- FETCH FOLLOWER COUNT -----------
def get_instagram_followers(username):
    url = f"https://api.scrapingdog.com/instagram?api_key={SCRAPINGDOG_API_KEY}&type=username&query={username}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get("followerCount")
    else:
        print(f"Error fetching followers: {response.status_code}, {response.text}")
        return None

# ----------- APPEND TO CSV -----------
def append_to_csv(file_path, date_str, time_str, followers, company):
    file_exists = os.path.exists(file_path)
    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["date", "time", "followers", "company"])
        writer.writerow([date_str, time_str, followers, company])

# ----------- MAIN -----------
def main():
    now = datetime.now()
    date_str = now.strftime("%d/%m/%Y")  # Day/Month/Year
    time_str = now.strftime("%H:%M:%S")  # 24-hour format
    followers = get_instagram_followers(INSTAGRAM_USERNAME)

    if followers is not None:
        append_to_csv(CSV_FILE_PATH, date_str, time_str, followers, INSTAGRAM_USERNAME)
        print(f"✅ Data appended: {date_str} {time_str} {followers} {INSTAGRAM_USERNAME}")
    else:
        print("⚠️ No data appended.")

if __name__ == "__main__":
    main()
