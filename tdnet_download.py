import os
import json
import time
import random
import logging
import requests
from typing import List, Dict, Any
from dataclasses import dataclass
from pypdf import PdfReader
from tqdm import tqdm

# Constants
CONFIG_FILE = "/root/src/config.json"
LOG_FILE = "/root/src/log/download.log"
EXTRACTED_DATA_FILE = "/root/src/log/extracted_data.json"
FAILED_DOWNLOADS_FILE = "/root/src/log/failed_downloads.json"
PDF_DIRECTORY = "/root/src/raw_data/timely-disclosure"

@dataclass
class Config:
    max_retries: int
    download_delay: tuple[int, int]
    url_template: str

def load_config() -> Config:
    with open(CONFIG_FILE, 'r') as f:
        config_data = json.load(f)
    return Config(**config_data)

def setup_logging():
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

class TdnetDownloader:
    def __init__(self, config: Config):
        self.config = config

    def fetch_data(self, url: str) -> List[Dict[str, Any]]:
        """Fetch data from the specified URL"""
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return [item["Tdnet"] for item in data["items"]]

    def extract_info(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Extract necessary information from fetched data"""
        return [
            {
                "pubdate": item["pubdate"],
                "company_code": item["company_code"],
                "document_url": item["document_url"][len("https://webapi.yanoshin.jp/rd.php?"):]
            } for item in data
        ]

    @staticmethod
    def save_data(data: Any, file_path: str):
        """Save data to a JSON file"""
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

    def download_pdf(self, url: str) -> bytes:
        """Download PDF file from the specified URL"""
        response = requests.get(url)
        response.raise_for_status()
        time.sleep(random.randint(*self.config.download_delay))
        return response.content

    @staticmethod
    def save_pdf(content: bytes, file_path: str):
        """Save downloaded PDF file"""
        with open(file_path, "wb") as f:
            f.write(content)

    @staticmethod
    def validate_pdf(file_path: str) -> bool:
        """Validate if the PDF file is correct"""
        try:
            with open(file_path, "rb") as f:
                pdf = PdfReader(f)
                return len(pdf.pages) > 0
        except Exception as e:
            logging.error(f"Error validating PDF at {file_path}: {e}")
            return False

    def process_downloads(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Process PDF downloads and saves"""
        failed_downloads = []
        with tqdm(total=len(data), desc="Downloading PDFs", unit="file") as progress_bar:
            for item in data:
                if item["company_code"][-1] == "0":
                    if self.download_single_pdf(item):
                        progress_bar.update(1)
                    else:
                        failed_downloads.append(item)
        return failed_downloads

    def download_single_pdf(self, item: Dict[str, str]) -> bool:
        """Download a single PDF file"""
        for _ in range(self.config.max_retries):
            try:
                pdf_content = self.download_pdf(item["document_url"])
                date = item["pubdate"].split(" ")[0]
                file_name = f"{date}_{item['company_code']}.pdf"
                file_path = os.path.join(PDF_DIRECTORY, file_name)

                self.save_pdf(pdf_content, file_path)

                if self.validate_pdf(file_path):
                    logging.info(f"Downloaded {file_path}.")
                    return True
                else:
                    os.remove(file_path)
                    logging.warning(f"Failed to validate {file_path}. Retrying download.")
            except Exception as e:
                logging.error(f"Failed to download {item['document_url']}: {e}")
        return False

    def retry_failed_downloads(self, failed_downloads: List[Dict[str, str]]):
        """Retry downloading failed files"""
        if not failed_downloads:
            logging.info("No failed downloads to retry.")
            return

        self.save_data(failed_downloads, FAILED_DOWNLOADS_FILE)
        self.process_downloads(failed_downloads)

    def run(self, mode: str = "today"):
        """
        Fetch data from TDnet and download PDF files
        mode: "today" - download only today's data, "month" - download last month's data
        """
        start_date, end_date = self.get_date_range(mode)
        url = self.config.url_template.format(start_date=start_date, end_date=end_date)
        
        data = self.fetch_data(url)
        extracted_data = self.extract_info(data)
        self.save_data(extracted_data, EXTRACTED_DATA_FILE)
        failed_downloads = self.process_downloads(extracted_data)
        self.retry_failed_downloads(failed_downloads)

    @staticmethod
    def get_date_range(mode: str) -> tuple[str, str]:
        """Get date range based on the specified mode"""
        if mode == "today":
            start_date = end_date = time.strftime("%Y%m%d")
        elif mode == "month":
            end_date = time.strftime("%Y%m%d")
            start_date = time.strftime("%Y%m%d", time.localtime(time.time() - 60 * 60 * 24 * 30))
        else:
            raise ValueError("Invalid mode. Choose either 'today' or 'month'.")
        return start_date, end_date

if __name__ == "__main__":
    setup_logging()
    config = load_config()
    downloader = TdnetDownloader(config)
    downloader.run()