# src/aeropulse/etl/pipelines/sync_mongo_weather_to_postgres.py
import os
import json
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.load.loader import mongo_client

logger = setup_logger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def main():
    load_dotenv()

    # Postgres target (curated table)
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    safe_url = make_url(dsn).set(password="***")
    logger.info("Connecting to DB (write curated): %s", safe_url)
    engine = create_engine(dsn, future=True)

    # Mongo source (raw)
    mongo_db_name = os.getenv("MONGO_DB")
    if not mongo_db_name:
        raise RuntimeError("Set MONGO_DB")
    mdb = mongo_client()[mongo_db_name]
    coll = mdb["weather_current_raw"]

    # Find which cells exist in Postgres
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT h3_res6 FROM public.weather_res6")).fetchall()
    if not rows:
        logger.info("No target cells in Postgres weather_res6.")
        return

    cells = [r[0] for r in rows]
    logger.info(
        "Syncing latest raw weather for %d cells from Mongo → Postgres.", len(cells)
    )

    updated = 0
    with engine.begin() as conn:
        for cell in cells:
            doc = coll.find_one({"h3_res6": cell}, sort=[("fetched_at", -1)])
            if not doc:
                continue

            payload = doc.get("payload")
            ts = doc.get("fetched_at") or datetime.now(timezone.utc)

            conn.execute(
                text(
                    """
                    UPDATE public.weather_res6
                    SET last_updated = :ts,
                        weather = CAST(:payload AS JSONB)
                    WHERE h3_res6 = :cell
                """
                ),
                {
                    "ts": ts,
                    "payload": (
                        json.dumps(payload)
                        if isinstance(payload, (dict, list))
                        else json.dumps({"value": payload})
                    ),
                    "cell": cell,
                },
            )
            updated += 1

    logger.info("Synced %d cell(s) from Mongo to Postgres.", updated)


if __name__ == "__main__":
    main()
