import argparse
import os
import sys
import requests

parser = argparse.ArgumentParser(description="Fetch news from NewsAPI")
parser.add_argument("--api-key", dest="api_key", help="NewsAPI API key (overrides env)")
parser.add_argument("--env-file", dest="env_file", help="Path to a file containing the API key (first line)")
parser.add_argument("--verbose", dest="verbose", action="store_true", help="Enable verbose output")
args = parser.parse_args()

api_key = args.api_key or os.getenv("NEWS_API_KEY")
if not api_key and args.env_file:
    try:
        with open(args.env_file, "r") as fh:
            api_key = fh.readline().strip()
    except Exception as e:
        print(f"Failed reading env file '{args.env_file}': {e}")
        sys.exit(1)

if not api_key:
    sys.exit("ERROR: set the NEWS_API_KEY environment variable or pass --api-key")

if args.verbose:
    masked = api_key[:4] + "..." if len(api_key) > 8 else "(key hidden)"
    print("Using API key:", masked)

url = "https://newsapi.org/v2/everything"
params = {
    "q": "terrorist AND attack",
    "language": "en",
    "pageSize": 100,
    "sortBy": "publishedAt",
    "apiKey": api_key,
}

response = requests.get(url, params=params, timeout=10)
try:
    response.raise_for_status()
except requests.HTTPError as e:
    print("HTTP error while calling NewsAPI:", e)
    sys.exit(1)

data = response.json()

if data.get("status") != "ok":
    print("NewsAPI returned non-ok status:", data.get("status"), data.get("message"))
    sys.exit(1)

for i in data.get('articles', []):
    print("Title:", i.get('title'))
    print("URL:", i.get('url'))
    print("Date:", i.get('publishedAt'))
    print("-" * 80)