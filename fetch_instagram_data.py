# fetch_instagram_data.py
import os
import requests
import csv
from datetime import datetime

# -------- CONFIG (same names you use in GitHub secrets) --------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
BUCKET_NAME         = "Marketing Database"
INSTAGRAM_USERNAME  = "extrastaff.recruitment"
CSV_FILE_PATH       = "followers_history.csv"


# -------- FETCH FOLLOWER COUNT --------
def get_instagram_followers(username):
    url = f"https://api.scrapingdog.com/instagram?api_key={SCRAPINGDOG_API_KEY}&type=username&query={username}"
    try:
        response = requests.get(url, timeout=20)
        print(f"🔍 API Request URL: {url}")
        print(f"🔍 Status: {response.status_code}")

        # log body (trim long ones)
        txt = response.text
        print(f"🔍 Response (first 300 chars): {txt[:300]}")

        if response.status_code == 200:
            data = response.json()
            follower_count = data.get("followerCount") or data.get("followers_count")

            if follower_count is not None:
                print(f"✅ Followers fetched successfully: {follower_count}")
                return int(follower_count)
            else:
                print("⚠️ followerCount key missing in response.")
                return None
        else:
            print(f"❌ Bad response from API: {response.status_code}")
            return None

    except Exception as e:
        print(f"❌ Exception during API call: {e}")
        return None


# -------- WRITE TO CSV --------
def append_to_csv(file_path, date_str, time_str, followers, company):
    try:
        file_exists = os.path.exists(file_path)
        with open(file_path, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["date", "time", "followers", "company"])
            writer.writerow([date_str, time_str, followers, company])
        print(f"📁 Appended to {file_path}")
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")


# -------- MAIN EXECUTION --------
def main():
    now = datetime.now()
    date_str = now.strftime("%d/%m/%Y")
    time_str = now.strftime("%H:%M:%S")

    print(f"🕒 Running Instagram fetch for {INSTAGRAM_USERNAME} on {date_str} {time_str}")

    followers = get_instagram_followers(INSTAGRAM_USERNAME)

    if followers is not None:
        append_to_csv(CSV_FILE_PATH, date_str, time_str, followers, INSTAGRAM_USERNAME)
        print(f"✅ Data saved: {date_str} {time_str} {followers} {INSTAGRAM_USERNAME}")
    else:
        print("⚠️ Skipped: Could not fetch followers, API returned None.")


if __name__ == "__main__":
    main()
