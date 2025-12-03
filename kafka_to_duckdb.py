import argparse
import json
import os
import signal
import time
import logging
import duckdb
from kafka import KafkaConsumer


def ensure_table(conn): # Creates db table to store articles 
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            title VARCHAR,
            url VARCHAR,
            published_at TIMESTAMP,
            source_name VARCHAR,
            author VARCHAR,
            description VARCHAR,
            content VARCHAR,
            raw_json VARCHAR
        )
        """
    )


def extract_row(article): # Extract relevant fields from the article JSON
    title = article.get("title")
    url = article.get("url")
    published_at = article.get("publishedAt")
    source_name = None
    if isinstance(article.get("source"), dict):
        source_name = article.get("source").get("name")
    author = article.get("author")
    description = article.get("description")
    content = article.get("content")
    raw = json.dumps(article)
    # dedupe_key: prefer url; fall back to title+published_at if url missing
    dedupe_key = url if url else f"{title}|{published_at}"
    return (title, url, published_at, source_name, author, description, content, raw, dedupe_key)


def main(): # Parse the input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default="news-articles")
    parser.add_argument("--group", default="news-consumer-group")
    parser.add_argument("--db", default="news.db", help="DuckDB file path")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug-kafka", action="store_true", help="Enable kafka client debug logging")
    args = parser.parse_args()

    # configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger("kafka_to_duckdb")
    logger.setLevel(log_level)

    if args.debug_kafka:
        logging.getLogger("kafka").setLevel(logging.DEBUG)
        logging.getLogger("kafka.conn").setLevel(logging.DEBUG)
        logger.debug("Kafka client debug logging enabled")

    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap.split(","),
            group_id=args.group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
    except Exception as e:
        logger.exception("Failed to create KafkaConsumer: %s", e)
        raise

    logger.info("KafkaConsumer created; topic=%s bootstrap=%s group=%s", args.topic, args.bootstrap, args.group)

    conn = duckdb.connect(database=args.db)
    ensure_table(conn)

    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        running = False
        logger.info("Received signal %s, shutting down", sig)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    buffer = []

    try:
        while running:
            for msg in consumer:
                article = msg.value
                row = extract_row(article)
                buffer.append(row)
                logger.debug("Buffered article: %s", row[0])

                if len(buffer) >= args.batch_size:
                    logger.info("Flushing %d rows to %s", len(buffer), args.db)
                    conn.execute("BEGIN TRANSACTION")
                    for r in buffer:
                        # r = (title, url, published_at, source_name, author, description, content, raw, dedupe_key)
                        title, url, published_at, source_name, author, description, content, raw, dedupe_key = r
                        try:
                            if url:
                                # insert only if url not already present
                                conn.execute(
                                    "INSERT INTO articles (title,url,published_at,source_name,author,description,content,raw_json) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM articles WHERE url = ?)",
                                    (title, url, published_at, source_name, author, description, content, raw, url),
                                )
                            else:
                                # fallback dedupe on title+published_at
                                conn.execute(
                                    "INSERT INTO articles (title,url,published_at,source_name,author,description,content,raw_json) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM articles WHERE title = ? AND published_at = ?)",
                                    (title, url, published_at, source_name, author, description, content, raw, title, published_at),
                                )
                        except Exception:
                            logger.exception("Failed to insert row for dedupe_key=%s", dedupe_key)
                    conn.execute("COMMIT")
                    logger.debug("Committed %d rows", len(buffer))
                    buffer.clear()

            # no messages received in this millisecond window
            if buffer:
                logger.info("Flushing %d rows to %s (final)", len(buffer), args.db)
                conn.execute("BEGIN TRANSACTION")
                for r in buffer:
                    title, url, published_at, source_name, author, description, content, raw, dedupe_key = r
                    try:
                        if url:
                            conn.execute(
                                "INSERT INTO articles (title,url,published_at,source_name,author,description,content,raw_json) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM articles WHERE url = ?)",
                                (title, url, published_at, source_name, author, description, content, raw, url),
                            )
                        else:
                            conn.execute(
                                "INSERT INTO articles (title,url,published_at,source_name,author,description,content,raw_json) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM articles WHERE title = ? AND published_at = ?)",
                                (title, url, published_at, source_name, author, description, content, raw, title, published_at),
                            )
                    except Exception:
                        logger.exception("Failed to insert row for dedupe_key=%s", dedupe_key)
                conn.execute("COMMIT")
                logger.debug("Committed final %d rows", len(buffer))
                buffer.clear()

            time.sleep(1)

    finally:
        try:
            consumer.close()
            logger.info("Kafka consumer closed")
        except Exception:
            logger.exception("Error closing Kafka consumer")
        try:
            conn.close()
            logger.info("DuckDB connection closed")
        except Exception:
            logger.exception("Error closing DuckDB connection")


if __name__ == "__main__":
    main()
