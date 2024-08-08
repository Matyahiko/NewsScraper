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

@dataclass
class Config:
    max_retries: int
    download_delay: tuple[int, int]
    url_template: str
    config_file: str
    log_file: str
    extracted_data_file: str
    failed_downloads_file: str
    pdf_directory: str

def load_config(config_path: str = "/home/higa/downloader/config.json") -> Config:
    """設定ファイルを読み込む"""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    return Config(**config_data)

def setup_logging(log_file: str):
    """ロギングの設定を行う"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_directories(config: Config):
    """必要なディレクトリが存在することを確認し、存在しない場合は作成する"""
    directories = [
        os.path.dirname(config.config_file),
        os.path.dirname(config.log_file),
        os.path.dirname(config.extracted_data_file),
        os.path.dirname(config.failed_downloads_file),
        config.pdf_directory
    ]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

class TdnetDownloader:
    def __init__(self, config: Config):
        self.config = config

    def fetch_data(self, url: str) -> List[Dict[str, Any]]:
        """指定されたURLからデータを取得する"""
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return [item["Tdnet"] for item in data["items"]]

    def extract_info(self, data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """取得したデータから必要な情報を抽出する"""
        return [
            {
                "pubdate": item["pubdate"],
                "company_code": item["company_code"],
                "document_url": item["document_url"][len("https://webapi.yanoshin.jp/rd.php?"):]
            } for item in data
        ]

    def save_data(self, data: Any, file_path: str):
        """データをJSONファイルに保存する"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

    def download_pdf(self, url: str) -> bytes:
        """指定されたURLからPDFファイルをダウンロードする"""
        response = requests.get(url)
        response.raise_for_status()
        time.sleep(random.randint(*self.config.download_delay))
        return response.content

    def save_pdf(self, content: bytes, file_path: str):
        """ダウンロードしたPDFファイルを保存する"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(content)

    def validate_pdf(self, file_path: str) -> bool:
        """PDFファイルが正しいかを検証する"""
        try:
            with open(file_path, "rb") as f:
                pdf = PdfReader(f)
                return len(pdf.pages) > 0
        except Exception as e:
            logging.error(f"PDFの検証中にエラーが発生しました {file_path}: {e}")
            return False

    def process_downloads(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """PDFのダウンロードと保存を処理する"""
        failed_downloads = []
        with tqdm(total=len(data), desc="PDFをダウンロード中", unit="ファイル") as progress_bar:
            for item in data:
                if item["company_code"][-1] == "0":
                    if self.download_single_pdf(item):
                        progress_bar.update(1)
                    else:
                        failed_downloads.append(item)
        return failed_downloads

    def download_single_pdf(self, item: Dict[str, str]) -> bool:
        """単一のPDFファイルをダウンロードする"""
        for _ in range(self.config.max_retries):
            try:
                pdf_content = self.download_pdf(item["document_url"])
                date = item["pubdate"].split(" ")[0]
                file_name = f"{date}_{item['company_code']}.pdf"
                file_path = os.path.join(self.config.pdf_directory, file_name)

                self.save_pdf(pdf_content, file_path)

                if self.validate_pdf(file_path):
                    logging.info(f"{file_path} をダウンロードしました。")
                    return True
                else:
                    os.remove(file_path)
                    logging.warning(f"{file_path} の検証に失敗しました。ダウンロードを再試行します。")
            except Exception as e:
                logging.error(f"{item['document_url']} のダウンロードに失敗しました: {e}")
        return False

    def retry_failed_downloads(self, failed_downloads: List[Dict[str, str]]):
        """ダウンロードに失敗したファイルを再試行する"""
        if not failed_downloads:
            logging.info("再試行するダウンロード失敗はありません。")
            return

        self.save_data(failed_downloads, self.config.failed_downloads_file)
        self.process_downloads(failed_downloads)

    def run(self, mode: str = "today"):
        """
        TDnetからデータを取得し、PDFファイルをダウンロードする
        mode: "today" - 今日のデータのみダウンロード, "month" - 先月のデータをダウンロード
        """
        start_date, end_date = self.get_date_range(mode)
        url = self.config.url_template.format(start_date=start_date, end_date=end_date)
        
        data = self.fetch_data(url)
        extracted_data = self.extract_info(data)
        self.save_data(extracted_data, self.config.extracted_data_file)
        failed_downloads = self.process_downloads(extracted_data)
        self.retry_failed_downloads(failed_downloads)

    @staticmethod
    def get_date_range(mode: str) -> tuple[str, str]:
        """指定されたモードに基づいて日付範囲を取得する"""
        if mode == "today":
            start_date = end_date = time.strftime("%Y%m%d")
        elif mode == "month":
            end_date = time.strftime("%Y%m%d")
            start_date = time.strftime("%Y%m%d", time.localtime(time.time() - 60 * 60 * 24 * 30))
        else:
            raise ValueError("無効なモードです。'today'か'month'を選択してください。")
        return start_date, end_date

if __name__ == "__main__":
    config = load_config()
    ensure_directories(config)
    setup_logging(config.log_file)
    downloader = TdnetDownloader(config)
    downloader.run()