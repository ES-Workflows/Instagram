import pandas as pd
import requests, os, logging
from datetime import datetime

# ---------- CONFIG ----------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
BUCKET_NAME         = "Marketing Database"   # ✅ keep as is, but no extra spaces in Supabase
INSTAGRAM_USERNAME  = "extrastaff.recruitment"

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.FileHandler("instagram_data_fetcher.log", encoding="utf-8"),
              logging.StreamHandler()]
)

# ---------- SUPABASE UPLOAD ----------
def upload_csv_to_supabase(path):
    """Uploads CSV to Supabase Storage; overwrites if exists."""
    name = os.path.basename(path)
    try:
        with open(path, "rb") as f:
            data = f.read()
        url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{name}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "text/csv",
            "x-upsert": "true"
        }
        r = requests.put(url, headers=headers, data=data, timeout=30)
        if r.status_code in (200, 201, 204):
            logging.info(f"✅ Uploaded {name} to Supabase.")
        else:
            logging.error(f"❌ Upload failed ({r.status_code}): {r.text}")
    except Exception as e:
        logging.error(f"Upload error for {name}: {e}")

# ---------- FETCH FOLLOWER COUNT ----------
def get_follower_count():
    try:
        r = requests.get(
            "https://api.scrapingdog.com/instagram/profile",
            params={"api_key": SCRAPINGDOG_API_KEY, "username": INSTAGRAM_USERNAME},
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
        count = data.get("followers_count")
        if count is None:
            logging.warning("⚠️ No follower count returned. Skipping save.")
            return None
        return int(count)
    except Exception as e:
        logging.error(f"Follower API error: {e}")
        return None

# ---------- SAVE FOLLOWER COUNT ----------
def save_follower_count(count):
    if count is None or count <= 0:
        logging.warning("⚠️ Skipping save — invalid follower count.")
        return

    path = "follower_history.csv"

    # ✅ Correct timestamp formatting here
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")   # consistent readable timestamp
    date_str = now.strftime("%d/%m/%Y")                 # clean date format for Power BI
    time_str = now.strftime("%H:%M:%S")                 # HH:MM:SS

    row = {
        "timestamp": timestamp_str,
        "date": date_str,
        "time": time_str,
        "followers_count": count,
        "username": INSTAGRAM_USERNAME
    }

    try:
        df_new = pd.DataFrame([row])
        if os.path.exists(path):
            df = pd.read_csv(path)
            df = pd.concat([df, df_new], ignore_index=True)
        else:
            df = df_new

        # remove invalid rows (0 or NaN followers)
        df = df[df["followers_count"] > 0].dropna(subset=["followers_count"])

        df.to_csv(path, index=False)
        logging.info(f"💾 Follower count saved: {count}")

        upload_csv_to_supabase(path)
        save_daily_summary(df)

    except Exception as e:
        logging.error(f"Error saving follower count: {e}")

# ---------- SAVE DAILY SUMMARY ----------
def save_daily_summary(df):
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        summary = (
            df.groupby("date")
            .agg(followers_count=("followers_count", "last"),
                 records_count=("timestamp", "count"))
            .reset_index()
        )
        summary["username"] = INSTAGRAM_USERNAME
        summary.to_csv("daily_follower_summary.csv", index=False)
        upload_csv_to_supabase("daily_follower_summary.csv")
        logging.info("📊 Daily summary updated.")
    except Exception as e:
        logging.error(f"Daily summary error: {e}")

# ---------- MAIN ----------
def main():
    logging.info("🚀 Starting Instagram follower fetch workflow...")
    count = get_follower_count()
    save_follower_count(count)
    logging.info("🎉 Completed successfully.")

if __name__ == "__main__":
    main()
