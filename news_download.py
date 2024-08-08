import os
import json
import csv
import hashlib
from datetime import datetime
import pytz
from newspaper import Article
import feedparser
import random
import traceback
import logging
from pathlib import Path
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import asyncio
import aiohttp
import aiofiles

# スクリプトのディレクトリを取得
SCRIPT_DIR = Path(__file__).parent.absolute()

# ログ設定
logging.basicConfig(
    filename=SCRIPT_DIR / 'log/news_scraper.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@task
def debug_log(message):
    logging.debug(message)

@task
async def read_rss_list_from_json(json_file):
    async with aiofiles.open(SCRIPT_DIR / json_file, mode="r", encoding="utf-8") as file:
        content = await file.read()
        return json.loads(content)

@task
async def fetch_article_text(session, url):
    try:
        async with session.get(url) as response:
            html = await response.text()
        
        article = Article(url)
        article.set_html(html)
        article.parse()
        return article.text
    except Exception as e:
        logging.error(f"Error fetching article from {url}: {e}")
        return None

@task
def generate_unique_id(url):
    return hashlib.md5(url.encode()).hexdigest()

@task
async def save_article_to_json(file_path, article_data):
    async with aiofiles.open(file_path, "w", encoding="utf-8") as json_file:
        await json_file.write(json.dumps(article_data, ensure_ascii=False, indent=2))

@task
async def save_index_to_csv(index_file, article_id, title, date, source, file_path):
    file_exists = os.path.isfile(index_file)
    async with aiofiles.open(index_file, "a", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            await csv_file.write("ID,Title,Date,Source,File Path\n")
        await csv_file.write(f"{article_id},{title},{date},{source},{file_path}\n")

@task
async def process_single_feed(session, rss_item, base_dir):
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
            text = await fetch_article_text(session, link)
            if text is None:
                continue
            
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
            await save_article_to_json(file_path, article_data)
            debug_log(f"Saved article to {file_path}")

            # Update index
            index_file = Path(base_dir) / "article_index.csv"
            await save_index_to_csv(index_file, article_id, title, date, source_name, str(file_path))
            debug_log(f"Updated index in {index_file}")

        except Exception as e:
            debug_log(f"Failed to process article from {link}. Error: {e}")
            debug_log(traceback.format_exc())

        await asyncio.sleep(random.uniform(3, 5))

@flow(task_runner=ConcurrentTaskRunner())
async def main_flow():
    base_dir = SCRIPT_DIR / "raw_data/news"
    base_dir.mkdir(parents=True, exist_ok=True)

    rss_list = await read_rss_list_from_json("RSS.json")
    debug_log(f"RSS List loaded: {rss_list}")

    async with aiohttp.ClientSession() as session:
        tasks = [process_single_feed(session, rss_item, base_dir) for rss_item in rss_list]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_flow())