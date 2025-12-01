import argparse
import json
import os
import sys
import time
from typing import List

from kafka import KafkaProducer
import requests


def get_api_key(args) -> str:
    api_key = args.api_key or os.getenv("NEWS_API_KEY")
    if args.env_file and not api_key:
        try:
            with open(args.env_file, "r") as fh:
                api_key = fh.readline().strip()
        except Exception as e:
            print(f"Failed to read env file: {e}")
            sys.exit(1)
    if not api_key:
        sys.exit("ERROR: provide NewsAPI key with --api-key or set NEWS_API_KEY")
    return api_key


def fetch_articles(api_key: str, page_size: int = 20) -> List[dict]:
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "terrorist OR mass shooting OR bombing OR terrorism",
        "language": "en",
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "apiKey": api_key,
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "ok":
        raise RuntimeError(f"NewsAPI error: {data.get('message')}")
    return data.get("articles", [])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", help="NewsAPI key (overrides env)")
    parser.add_argument("--env-file", help="Read API key from file (first line)")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                        help="Kafka bootstrap servers (comma separated)")
    parser.add_argument("--topic", default="news-articles", help="Kafka topic to publish to")
    parser.add_argument("--page-size", type=int, default=100, help="How many articles to fetch per request")
    parser.add_argument("--interval", type=int, default=0, help="If >0, poll every N seconds")
    parser.add_argument("--key-field", default="url", help="Article field to use as message key (optional)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    api_key = get_api_key(args)

    if args.verbose:
        masked = api_key[:4] + "..." if len(api_key) > 8 else "(hidden)"
        print(f"Using NewsAPI key: {masked}")
        print(f"Kafka bootstrap: {args.bootstrap}")
        print(f"Producing to topic: {args.topic}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
        retries=5,
    )

    try:
        while True:
            try:
                articles = fetch_articles(api_key, page_size=args.page_size)
            except Exception as e:
                print("Failed fetching articles:", e)
                if args.interval <= 0:
                    sys.exit(1)
                time.sleep(args.interval)
                continue

            if args.verbose:
                print(f"Fetched {len(articles)} articles")

            for a in articles:
                key = None
                if args.key_field:
                    key = a.get(args.key_field)
                producer.send(args.topic, key=key, value=a)
                if args.verbose:
                    print("Sent article:", a.get("title"))

            producer.flush()

            if args.interval <= 0:
                break
            time.sleep(args.interval)
    finally:
        try:
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
