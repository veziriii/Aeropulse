import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.load.loader.mongo_loader import latest_docs_by_keys
from aeropulse.etl.load.loader.pg_loader import upsert_jsonb_rows, masked_dsn_for_log

logger = setup_logger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def main():
    load_dotenv()

    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")

    logger.info("Connecting to DB (write curated): %s", masked_dsn_for_log())
    engine = create_engine(dsn, future=True)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT h3_res6 FROM public.weather_res6")).fetchall()
    if not rows:
        logger.info("No target cells in Postgres weather_res6.")
        return

    cells: List[str] = [r[0] for r in rows]
    logger.info(
        "Syncing latest raw weather for %d cells (Mongo → Postgres).", len(cells)
    )

    latest_map = latest_docs_by_keys(
        "weather_current_raw",
        key_field="h3_res6",
        keys=cells,
        sort_field="fetched_at",
    )

    payload_rows = []
    for cell, doc in latest_map.items():
        payload = doc.get("payload")
        ts = doc.get("fetched_at") or datetime.now(timezone.utc)
        if payload is None:
            continue
        payload_rows.append(
            {
                "pk": cell,
                "payload": (
                    json.dumps(payload) if not isinstance(payload, str) else payload
                ),
                "ts": ts,
            }
        )

    if not payload_rows:
        logger.info("No new payloads to sync.")
        return

    updated = upsert_jsonb_rows(
        table="public.weather_res6",
        pk_column="h3_res6",
        jsonb_column="weather",
        ts_column="last_updated",
        rows=payload_rows,
    )

    logger.info("Upserted %d curated snapshot(s) into Postgres.", updated)


if __name__ == "__main__":
    main()
