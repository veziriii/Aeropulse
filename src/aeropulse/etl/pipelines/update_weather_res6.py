import os
import math
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

import h3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from dotenv import load_dotenv

from aeropulse.services.openweather_client import OpenWeatherClient
from aeropulse.utils.rate_limit import DailyBudget, env_daily_budget
from aeropulse.utils.logging_config import setup_logger

logger = setup_logger(__name__)
# Hide full request URLs (with appid) from logs
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def h3_to_latlon(cell: str) -> Tuple[float, float]:
    """H3 v3/v4 compatibility to get cell center."""
    if hasattr(h3, "h3_to_geo"):
        lat, lon = h3.h3_to_geo(cell)
    elif hasattr(h3, "cell_to_latlng"):
        lat, lon = h3.cell_to_latlng(cell)
    else:
        raise RuntimeError("No suitable H3 function found (h3_to_geo/cell_to_latlng).")
    if not (math.isfinite(lat) and math.isfinite(lon)):
        raise ValueError("Invalid lat/lon from H3 center.")
    return float(lat), float(lon)


def main():
    load_dotenv()

    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")

    # Mask password when logging the DSN
    safe_url = make_url(dsn).set(password="***")
    logger.info("Connecting to DB: %s", safe_url)

    # Freshness policy
    freshness_minutes = int(os.getenv("WEATHER_FRESH_MINUTES", "30"))
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=freshness_minutes)

    # Daily budget & pacing
    daily_limit = env_daily_budget(900)  # keep safely under 1000/day
    min_interval = float(os.getenv("OPENWEATHER_MIN_INTERVAL_SEC", "0.1"))
    budget = DailyBudget(daily_limit=daily_limit, min_interval_sec=min_interval)

    engine = create_engine(dsn, future=True)
    Session = sessionmaker(bind=engine, future=True)

    client = OpenWeatherClient()

    per_run_cap = int(os.getenv("WEATHER_UPDATE_BATCH", "500"))
    to_do = min(per_run_cap, budget.remaining())
    if to_do <= 0:
        logger.info("Budget exhausted for today. Skipping.")
        return

    # Select stale or never-updated cells
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
            {"cutoff": cutoff, "lim": to_do},
        ).fetchall()

    if not rows:
        logger.info(
            "No stale cells to update (freshness=%d minutes).", freshness_minutes
        )
        return

    logger.info("Updating up to %d cells (found %d candidates)", to_do, len(rows))

    updated = 0
    with Session() as session:
        for (cell,) in rows:
            if budget.remaining() <= 0:
                logger.info("Budget exhausted mid-run at %d updated.", updated)
                break

            try:
                lat, lon = h3_to_latlon(cell)
            except Exception as e:
                logger.warning("Skip invalid H3 cell %s: %s", cell, e)
                continue

            budget.wait_min_interval()

            try:
                data = client.current(
                    lat, lon, units=os.getenv("OWM_UNITS", "standard")
                )
            except RuntimeError as e:
                if "401 Unauthorized" in str(e):
                    logger.error("%s", e)
                    logger.error(
                        "Stopping run because the key is unauthorized. Check OPENWEATHER_API_KEY."
                    )
                    break
                logger.warning("OpenWeather fail for %s (%s,%s): %s", cell, lat, lon, e)
                continue
            except Exception as e:
                logger.warning(
                    "OpenWeather error for %s (%s,%s): %s", cell, lat, lon, e
                )
                continue

            now_utc = datetime.now(timezone.utc)

            # Serialize dict -> JSON text, cast to JSONB on the server
            session.execute(
                text(
                    """
                    UPDATE public.weather_res6
                    SET last_updated = :ts,
                        weather = CAST(:payload AS JSONB)
                    WHERE h3_res6 = :cell
                """
                ),
                {
                    "ts": now_utc,
                    "payload": json.dumps(data),
                    "cell": cell,
                },
            )

            updated += 1
            budget.consume(1)

        session.commit()

    logger.info(
        "Updated %d cell(s). Remaining daily budget: %d", updated, budget.remaining()
    )


if __name__ == "__main__":
    main()
