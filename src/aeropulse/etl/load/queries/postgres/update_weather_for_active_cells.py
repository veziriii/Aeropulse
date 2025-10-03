import os
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import text
from aeropulse.etl.load.loader.pg_loader import get_engine, masked_dsn_for_log
from aeropulse.etl.load.loader.mongo_loader import get_collection, insert_batch
from aeropulse.services.openweather_client import OpenWeatherClient
from aeropulse.utils.logging_config import setup_logger
from aeropulse.utils.parquet_io import write_parquet_partitioned

logger = setup_logger(__name__)

FRESH_MIN = int(os.getenv("WEATHER_FRESH_MINUTES", "45"))
BATCH_CAP = int(os.getenv("WEATHER_ACTIVE_CELLS_CAP", "250"))


def main():
    eng = get_engine()
    ow = OpenWeatherClient()

    # 1) recent cells from states (last 20 min)
    with eng.begin() as con:
        rows = con.execute(
            text(
                """
            SELECT DISTINCT h3_res6
            FROM public.opensky_states
            WHERE ts >= now() - interval '20 minutes'
              AND h3_res6 IS NOT NULL
            LIMIT :cap
        """
            ),
            {"cap": BATCH_CAP},
        ).fetchall()
    cells = [r[0] for r in rows]
    if not cells:
        logger.info("No active cells.")
        return

    # 2) staleness check against weather_res6
    with eng.begin() as con:
        stale = con.execute(
            text(
                f"""
            SELECT c.h3_res6
            FROM (SELECT UNNEST(:cells) AS h3_res6) c
            LEFT JOIN public.weather_res6 w ON w.h3_res6 = c.h3_res6
            WHERE w.last_updated IS NULL OR w.last_updated < now() - interval '{FRESH_MIN} minutes'
        """
            ),
            {"cells": cells},
        ).fetchall()
    stale_cells = [r[0] for r in stale]
    if not stale_cells:
        logger.info("All active cells are fresh.")
        return

    logger.info(
        "Refreshing %d/%d active cells. DB=%s",
        len(stale_cells),
        len(cells),
        masked_dsn_for_log(),
    )

    # 3) fetch weather and write Mongo raw + Postgres history/latest + Parquet
    weather_coll = get_collection("weather_raw")
    hist_rows = []
    latest_rows = []
    parquet_rows = []
    now = datetime.now(timezone.utc)

    for cell in stale_cells:
        lat, lon = ow.h3_cell_center(cell)
        data = ow.get_current(lat, lon)  # dict
        mongo_doc = {
            "h3_res6": cell,
            "fetched_at": now,
            "lat": lat,
            "lon": lon,
            "payload": data,
        }
        parquet_rows.append(
            {
                "h3_res6": cell,
                "fetched_at": now,
                "lat": lat,
                "lon": lon,
                "payload": data,
            }
        )

        hist_rows.append({"h3_res6": cell, "fetched_at": now, "weather": data})
        latest_rows.append({"h3_res6": cell, "last_updated": now, "weather": data})

        # batch Mongo insert in-memory; we’ll insert once after loop
    insert_batch(weather_coll, (dict(d) for d in parquet_rows))

    # parquet
    df = pd.DataFrame(parquet_rows)
    out_dir = os.getenv("PROCESSED_DIR", "data/processed/weather_current")
    if not df.empty:
        df2 = df.assign(
            dt=df["fetched_at"].dt.strftime("%Y-%m-%d"),
            hour=df["fetched_at"].dt.strftime("%H"),
        )
        write_parquet_partitioned(df2, out_dir, ["dt", "hour"])

    # Postgres history + latest
    with eng.begin() as con:
        con.execute(
            text(
                """
            INSERT INTO public.weather_res6_history (h3_res6, fetched_at, weather)
            VALUES (:h3_res6, :fetched_at, CAST(:weather AS JSONB))
        """
            ),
            [
                {
                    "h3_res6": r["h3_res6"],
                    "fetched_at": r["fetched_at"],
                    "weather": r["weather"],
                }
                for r in hist_rows
            ],
        )

        con.execute(
            text(
                """
            INSERT INTO public.weather_res6 (h3_res6, last_updated, weather)
            VALUES (:h3_res6, :last_updated, CAST(:weather AS JSONB))
            ON CONFLICT (h3_res6)
            DO UPDATE SET last_updated = EXCLUDED.last_updated, weather = EXCLUDED.weather
        """
            ),
            [
                {
                    "h3_res6": r["h3_res6"],
                    "last_updated": r["last_updated"],
                    "weather": r["weather"],
                }
                for r in latest_rows
            ],
        )

    logger.info("Weather refresh complete for %d cells.", len(stale_cells))


if __name__ == "__main__":
    main()
