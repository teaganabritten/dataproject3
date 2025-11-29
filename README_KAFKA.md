# Kafka + DuckDB integration (NewsAPI)

This folder contains helper scripts to fetch NewsAPI data, publish it to Kafka, and consume it into a local DuckDB database for analysis.

Setup
1. Start Kafka + Zookeeper (uses `docker-compose.yml` in this folder):

```bash
docker compose up -d
```

2. Create a Python virtualenv and install requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Producer
- `newsapi_producer.py` fetches articles and publishes JSON messages to the Kafka topic `news-articles` by default.

Example:
```bash
export NEWS_API_KEY="your_api_key_here"
python3 newsapi_producer.py --bootstrap localhost:9092 --topic news-articles --verbose
```

Consumer → DuckDB
- `kafka_to_duckdb.py` consumes messages and appends them into `news.db` table `articles`.

Example:
```bash
python3 kafka_to_duckdb.py --bootstrap localhost:9092 --topic news-articles --verbose
```

Inspect data with DuckDB CLI or Python:
```bash
# run duckdb interactive shell
duckdb news.db

# in shell
SELECT count(*) FROM articles;
SELECT title, published_at FROM articles ORDER BY published_at DESC LIMIT 10;
```

Security notes
- Do not commit `NEWS_API_KEY` or any secret to git. Use `.env` files or pass `--api-key` at runtime for short tests.
