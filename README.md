# News Scraper

## 概要

News Scraperは、指定されたRSSフィードから記事を自動的に収集し、保存するPythonスクリプトです。
適時開示に対応予定。

## 主な機能

- 複数のRSSフィードからニュース記事を収集
- 記事の本文をスクレイピング
- 記事データをJSON形式で保存
- 収集した記事のインデックスをCSVファイルで管理
- ログ機能によるスクレイピング過程の追跡

## 必要条件

- Python 3.6以上
- 以下のPythonライブラリ:
  - feedparser
  - newspaper3k
  - pytz

## インストール

1. このリポジトリをクローンまたはダウンロードします。

```
git clone https://github.com/yourusername/news-scraper.git
cd news-scraper
```

2. 必要なライブラリをインストールします。

```
pip install -r requirements.txt
```

## 使用方法

1. `RSS.json`ファイルにスクレイピングしたいRSSフィードのURLとソース名を追加します。

```json
[
  {
    "name": "Example News",
    "url": "http://example.com/rss"
  },
  {
    "name": "Another News Source",
    "url": "http://anothernews.com/feed"
  }
]
```

2. スクリプトを実行します。

```
python news_scraper.py
```

3. スクレイピングされた記事は`raw_data/news/[ソース名]/[年-月]/[記事ID].json`に保存されます。

4. 記事のインデックスは`raw_data/news/article_index.csv`に保存されます。

## 設定

- スクレイピングの間隔を調整するには、`random_sleep`関数の引数を変更します。
- ログレベルを変更するには、`logging.basicConfig`の`level`パラメータを調整します。

## 注意事項

- このスクリプトを使用する際は、ターゲットウェブサイトの利用規約を遵守してください。
- 過度に頻繁なリクエストは避け、サーバーに負荷をかけないようにしてください。
