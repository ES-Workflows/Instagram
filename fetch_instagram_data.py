import os
import csv
from datetime import datetime
import requests
from dotenv import load_dotenv

# ---------- LOAD SECRETS ----------
load_dotenv()

SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
BUCKET_NAME         = "Marketing Database"
INSTAGRAM_USERNAME  = "extrastaff.recruitment"

# ---------- FILE CONFIG ----------
CSV_FILE = "followers_history.csv"

# ---------- FETCH FOLLOWERS COUNT ----------
def get_followers_count(username):
    url = f"https://api.scrapingdog.com/instagram?api_key={SCRAPINGDOG_API_KEY}&profile={username}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("followers", None)
    except Exception as e:
        print(f"❌ Failed to fetch followers for {username}: {e}")
        return None

# ---------- SAVE TO CSV ----------
def append_to_csv(date_str, time_str, followers, username):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Date", "Time", "Followers Count", "Company Name"])
        writer.writerow([date_str, time_str, followers, username])
    print(f"✅ Appended: {date_str}, {time_str}, {followers}, {username}")

# ---------- MAIN ----------
def main():
    followers = get_followers_count(INSTAGRAM_USERNAME)
    if followers is None:
        print("⚠️ No followers data fetched. Exiting.")
        return

    now = datetime.now()
    date_str = now.strftime("%d/%m/%Y")
    time_str = now.strftime("%H:%M:%S")
    append_to_csv(date_str, time_str, followers, INSTAGRAM_USERNAME)

if __name__ == "__main__":
    main()
