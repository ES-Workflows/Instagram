import pandas as pd
import requests, os, logging
from datetime import datetime
import numpy as np

# ---------- CONFIG ----------
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY")
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
BUCKET_NAME         = "marketing-database"   # ❌ no spaces!
INSTAGRAM_USERNAME  = "extrastaff.recruitment"
INSTAGRAM_USER_ID   = "75427613659"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.FileHandler("instagram_data_fetcher.log", encoding="utf-8"),
              logging.StreamHandler()]
)

# ---------- SUPABASE UPLOAD ----------
def upload_csv_to_supabase(path):
    """PUTs CSV to Supabase Storage; overwrites if exists."""
    name = os.path.basename(path)
    try:
        with open(path, "rb") as f: data = f.read()
        url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{name}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "text/csv",
            "x-upsert": "true"
        }
        r = requests.put(url, headers=headers, data=data, timeout=30)
        if r.status_code in (200,201,204):
            logging.info(f"✅ Uploaded {name} to Supabase.")
        else:
            logging.error(f"❌ Upload failed ({r.status_code}): {r.text}")
    except Exception as e:
        logging.error(f"Upload error for {name}: {e}")

# ---------- FOLLOWER COUNT ----------
def get_follower_count():
    try:
        r = requests.get("https://api.scrapingdog.com/instagram/profile",
                         params={"api_key": SCRAPINGDOG_API_KEY, "username": INSTAGRAM_USERNAME},
                         timeout=15)
        r.raise_for_status()
        data = r.json()
        return int(data.get("followers_count", 0))
    except Exception as e:
        logging.error(f"Follower API error: {e}")
        return 0

def save_follower_count(count):
    path = "follower_history.csv"
    now  = datetime.now()
    row  = {"timestamp": now, "date": now.date(), "time": now.time(),
            "followers_count": count, "username": INSTAGRAM_USERNAME}

    try:
        df_new = pd.DataFrame([row])
        if os.path.exists(path):
            df = pd.read_csv(path)
            df = pd.concat([df, df_new], ignore_index=True)
        else:
            df = df_new
        df.to_csv(path, index=False)
        upload_csv_to_supabase(path)
        save_daily_summary(df)
        logging.info(f"Follower count saved ({count})")
    except Exception as e:
        logging.error(f"Error saving follower count: {e}")

def save_daily_summary(df):
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        summary = (df.groupby("date")
                     .agg(followers_count=("followers_count","last"),
                          records_count=("timestamp","count"))
                     .reset_index())
        summary["username"] = INSTAGRAM_USERNAME
        summary.to_csv("daily_follower_summary.csv", index=False)
        upload_csv_to_supabase("daily_follower_summary.csv")
    except Exception as e:
        logging.error(f"Daily summary error: {e}")

# ---------- POSTS ----------
def fetch_instagram_posts():
    """Fetch new posts and merge with existing ig.csv."""
    url = "https://api.scrapingdog.com/instagram/posts"
    user_id = INSTAGRAM_USER_ID
    all_posts, next_token = [], None

    logging.info("📡 Fetching Instagram posts...")
    while True:
        p = {"api_key": SCRAPINGDOG_API_KEY, "id": user_id}
        if next_token: p["next_page_token"] = next_token
        r = requests.get(url, params=p, timeout=30)
        if r.status_code != 200:
            logging.error(f"Post request failed: {r.status_code}")
            break
        j = r.json()
        batch = j.get("posts_data", [])
        all_posts.extend(batch)
        nxt = j.get("next_page_token", {})
        if not nxt.get("has_next_page"): break
        next_token = nxt.get("token")

    logging.info(f"Fetched {len(all_posts)} posts.")
    if all_posts:
        process_and_save_posts(all_posts)
    else:
        logging.warning("No posts fetched.")

def process_and_save_posts(posts):
    try:
        rows=[]
        for it in posts:
            owner=it.pop("owner",{}) or {}
            it["owner_id"]=owner.get("id")
            it["owner_username"]=owner.get("username")
            rows.append(it)
        new=pd.DataFrame(rows)
        if "likes" in new.columns:
            new["likes"]=new["likes"].replace(-1,0)

        # merge with existing file
        if os.path.exists("ig.csv"):
            old=pd.read_csv("ig.csv")
            df=pd.concat([old,new],ignore_index=True)
            df.drop_duplicates(subset=["id"],inplace=True)
        else:
            df=new

        df.to_csv("ig.csv",index=False)
        upload_csv_to_supabase("ig.csv")
        create_processed_posts_data(df)
        logging.info(f"Saved {len(df)} posts to ig.csv")
    except Exception as e:
        logging.error(f"Post processing error: {e}")

def create_processed_posts_data(df):
    try:
        if df.empty: return
        df["datetime"]=pd.to_datetime(df["timestamp"],errors="coerce")
        df["instagram_url"]=df.get("shortcode",df.get("id","")).apply(lambda s:f"https://www.instagram.com/p/{s}/")
        keep=["datetime","shortcode","likes","comments","caption","is_video","instagram_url"]
        df[[c for c in keep if c in df.columns]].sort_values("datetime",ascending=False)\
          .to_csv("instagram_posts_processed.csv",index=False)
        upload_csv_to_supabase("instagram_posts_processed.csv")
    except Exception as e:
        logging.error(f"Processed posts error: {e}")

# ---------- MAIN ----------
def main():
    logging.info("🚀 Starting Instagram fetch workflow...")
    count=get_follower_count()
    save_follower_count(count)
    fetch_instagram_posts()
    logging.info("🎉 Completed successfully.")

if __name__=="__main__":
    main()
