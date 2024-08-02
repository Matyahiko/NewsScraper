import os
import json
import csv
import hashlib
from datetime import datetime
import pytz
from newspaper import Article
import feedparser
import random
import time
import traceback
import logging
from pathlib import Path

# スクリプトのディレクトリを取得
SCRIPT_DIR = Path(__file__).parent.absolute()

# ログ設定
logging.basicConfig(
    filename=SCRIPT_DIR / 'log/news_scraper.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def debug_log(message):
    logging.debug(message)

def read_rss_list_from_json(json_file):
    with open(SCRIPT_DIR / json_file, mode="r", encoding="utf-8") as file:
        return json.load(file)

def random_sleep(min_sec=5, max_sec=15):
    sleep_time = random.uniform(min_sec, max_sec)
    time.sleep(sleep_time)

def fetch_article_text(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    article = Article(url)
    article.headers = headers
    article.download()
    article.parse()
    return article.text

def generate_unique_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def save_article_to_json(file_path, article_data):
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(article_data, json_file, ensure_ascii=False, indent=2)

def save_index_to_csv(index_file, article_id, title, date, source, file_path):
    file_exists = os.path.isfile(index_file)
    with open(index_file, "a", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(["ID", "Title", "Date", "Source", "File Path"])
        writer.writerow([article_id, title, date, source, file_path])

def process_feed(rss_item, base_dir):
    source_name = rss_item["name"]
    feed_url = rss_item["url"]
    debug_log(f"Processing RSS feed from {source_name}")

    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        debug_log(f"Failed to parse RSS from {feed_url}. Error: {e}")
        return

    for index, entry in enumerate(feed.entries):
        title = entry.title
        date = entry.get("published", entry.get("updated", ""))
        link = entry.link
        article_id = generate_unique_id(link)

        debug_log(f"Processing article {index + 1}/{len(feed.entries)}")
        debug_log(f"Title: {title}")
        debug_log(f"Date: {date}")
        debug_log(f"Link: {link}")
        debug_log(f"Article ID: {article_id}")

        try:
            text = fetch_article_text(link)
            debug_log(f"First 100 chars of the text: {text[:100]}")

            # Create directory structure
            jst = pytz.timezone('Asia/Tokyo')
            year_month = datetime.now(jst).strftime("%Y-%m")
            article_dir = Path(base_dir) / source_name / year_month
            article_dir.mkdir(parents=True, exist_ok=True)

            # Save article as JSON
            file_path = article_dir / f"{article_id}.json"
            article_data = {
                "id": article_id,
                "title": title,
                "date": date,
                "link": link,
                "text": text,
                "source": source_name
            }
            save_article_to_json(file_path, article_data)
            debug_log(f"Saved article to {file_path}")

            # Update index
            index_file = Path(base_dir) / "article_index.csv"
            save_index_to_csv(index_file, article_id, title, date, source_name, str(file_path))
            debug_log(f"Updated index in {index_file}")

        except Exception as e:
            debug_log(f"Failed to process article from {link}. Error: {e}")
            debug_log(traceback.format_exc())

        random_sleep()

if __name__ == "__main__":
    base_dir = SCRIPT_DIR / "raw_data/news"
    base_dir.mkdir(parents=True, exist_ok=True)

    rss_list = read_rss_list_from_json("RSS.json")
    debug_log(f"RSS List loaded: {rss_list}")

    for rss_item in rss_list:
        process_feed(rss_item, base_dir)