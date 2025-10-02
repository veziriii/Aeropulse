# src/aeropulse/etl/pipelines/fetch_weather_to_mongo.py
import os
import math
import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

import h3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from aeropulse.services.openweather_client import OpenWeatherClient
from aeropulse.utils.rate_limit import DailyBudget, env_daily_budget
from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.load.loader import mongo_client  # you already have this helper

logger = setup_logger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def h3_to_latlon(cell: str) -> Tuple[float, float]:
    if hasattr(h3, "h3_to_geo"):
        lat, lon = h3.h3_to_geo(cell)
    elif hasattr(h3, "cell_to_latlng"):
        lat, lon = h3.cell_to_latlng(cell)
    else:
        raise RuntimeError("No suitable H3 function found")
    if not (math.isfinite(lat) and math.isfinite(lon)):
        raise ValueError("Invalid lat/lon from H3 center.")
    return float(lat), float(lon)


def main():
    load_dotenv()

    # Postgres connection (only to read which cells are stale)
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    safe_url = make_url(dsn).set(password="***")
    logger.info("Connecting to DB (read cells): %s", safe_url)

    # Mongo target (raw landing)
    mongo_db_name = os.getenv("MONGO_DB")
    if not mongo_db_name:
        raise RuntimeError("Set MONGO_DB")
    mdb = mongo_client()[mongo_db_name]
    coll = mdb["weather_current_raw"]  # <<— raw collection

    # Ensure helpful indexes (idempotent)
    coll.create_index([("h3_res6", 1), ("fetched_at", -1)], background=True)
    coll.create_index([("fetched_at", -1)], background=True)

    # Freshness + budgets
    freshness_minutes = int(os.getenv("WEATHER_FRESH_MINUTES", "30"))
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=freshness_minutes)

    daily_limit = env_daily_budget(900)  # keep under 1000/day
    min_interval = float(os.getenv("OPENWEATHER_MIN_INTERVAL_SEC", "0.1"))
    budget = DailyBudget(daily_limit=daily_limit, min_interval_sec=min_interval)

    per_run_cap = int(os.getenv("WEATHER_UPDATE_BATCH", "500"))

    # Get stale cells from Postgres
    engine = create_engine(dsn, future=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT h3_res6
                FROM public.weather_res6
                WHERE last_updated IS NULL OR last_updated < :cutoff
                ORDER BY (last_updated IS NULL) DESC, last_updated ASC
                LIMIT :lim
            """
            ),
            {"cutoff": cutoff, "lim": min(per_run_cap, budget.remaining())},
        ).fetchall()

    if not rows:
        logger.info("No stale cells to fetch (freshness=%d min).", freshness_minutes)
        return

    client = OpenWeatherClient()
    units = os.getenv("OWM_UNITS", "standard")

    fetched = 0
    for (cell,) in rows:
        if budget.remaining() <= 0:
            logger.info("Budget exhausted after %d fetches.", fetched)
            break

        try:
            lat, lon = h3_to_latlon(cell)
        except Exception as e:
            logger.warning("Skip invalid H3 cell %s: %s", cell, e)
            continue

        budget.wait_min_interval()

        try:
            payload = client.current(lat, lon, units=units)
        except RuntimeError as e:
            if "401 Unauthorized" in str(e):
                logger.error("%s", e)
                logger.error("Stopping run (bad API key).")
                break
            logger.warning("OpenWeather fail for %s (%s,%s): %s", cell, lat, lon, e)
            continue
        except Exception as e:
            logger.warning("OpenWeather error for %s (%s,%s): %s", cell, lat, lon, e)
            continue

        doc = {
            "h3_res6": cell,
            "lat": lat,
            "lon": lon,
            "units": units,
            "source": "openweather_current",
            "fetched_at": datetime.now(timezone.utc),
            "payload": payload,  # raw JSON as dict
        }
        coll.insert_one(doc)  # APPEND-ONLY (keeps history)

        fetched += 1
        budget.consume(1)

    logger.info(
        "Fetched %d raw weather docs to Mongo. Remaining budget: %d",
        fetched,
        budget.remaining(),
    )


if __name__ == "__main__":
    main()
