import os
from datetime import datetime, timezone, timedelta
import pandas as pd
from sqlalchemy import text
from aeropulse.etl.load.loader.pg_loader import get_engine, masked_dsn_for_log
from aeropulse.utils.logging_config import setup_logger
from aeropulse.utils.parquet_io import write_parquet_partitioned

logger = setup_logger(__name__)

# how far we’re willing to look for the closest weather snapshot
MATCH_TOL_MIN = int(os.getenv("WEATHER_MATCH_TOL_MIN", "15"))


def main():
    eng = get_engine()
    now = datetime.now(timezone.utc)
    t0 = now - timedelta(hours=1)

    # Pull last-hour states
    with eng.begin() as con:
        states = (
            con.execute(
                text(
                    """
            SELECT ts, icao24, callsign, h3_res6
            FROM public.opensky_states
            WHERE ts >= now() - interval '1 hour'
              AND h3_res6 IS NOT NULL
        """
                )
            )
            .mappings()
            .all()
        )

        # Pull relevant weather history for those cells within tolerance window
        cells = list({r["h3_res6"] for r in states})
        if not cells:
            logger.info("No states in last hour.")
            return

        weather = (
            con.execute(
                text(
                    """
            SELECT h3_res6, fetched_at, weather
            FROM public.weather_res6_history
            WHERE h3_res6 = ANY(:cells)
              AND fetched_at >= now() - interval '2 hours'
        """
                ),
                {"cells": cells},
            )
            .mappings()
            .all()
        )

    if not states or not weather:
        logger.info("Not enough data to join.")
        return

    df_s = pd.DataFrame(states)
    df_w = pd.DataFrame(weather)

    # For each state row, pick nearest-in-time weather on same cell
    df_s["key"] = df_s["h3_res6"]
    df_w["key"] = df_w["h3_res6"]

    # Sort and asof-merge by time per cell
    df_s = df_s.sort_values(["key", "ts"]).reset_index(drop=True)
    df_w = df_w.sort_values(["key", "fetched_at"]).reset_index(drop=True)

    # group-based nearest merge
    out_rows = []
    tol = pd.Timedelta(minutes=MATCH_TOL_MIN)
    for cell, g_s in df_s.groupby("key"):
        g_w = df_w[df_w["key"] == cell]
        if g_w.empty:
            continue
        merged = pd.merge_asof(
            g_s.sort_values("ts"),
            g_w[["fetched_at", "weather"]].sort_values("fetched_at"),
            left_on="ts",
            right_on="fetched_at",
            direction="nearest",
            tolerance=tol,
        )
        merged["h3_res6"] = cell
        out_rows.append(merged)

    if not out_rows:
        logger.info("No matches within ±%d minutes.", MATCH_TOL_MIN)
        return

    out = pd.concat(out_rows, ignore_index=True)
    out = out.dropna(subset=["fetched_at"])  # keep only matched rows
    out["weather_summary"] = out["weather"].apply(
        lambda w: w.get("weather", [{}])[0].get("main") if isinstance(w, dict) else None
    )

    # Write to Postgres
    with eng.begin() as con:
        con.execute(
            text(
                """
            INSERT INTO public.flight_weather_hits
                (ts_state, icao24, callsign, h3_res6, weather_at, weather, weather_summary)
            VALUES
                (:ts_state, :icao24, :callsign, :h3_res6, :weather_at, CAST(:weather AS JSONB), :weather_summary)
        """
            ),
            [
                {
                    "ts_state": r["ts"],
                    "icao24": r["icao24"],
                    "callsign": r["callsign"],
                    "h3_res6": r["h3_res6"],
                    "weather_at": r["fetched_at"],
                    "weather": r["weather"],
                    "weather_summary": r["weather_summary"],
                }
                for _, r in out.iterrows()
            ],
        )

    # Parquet output for viz
    out_dir = os.getenv("PROCESSED_DIR", "data/processed/flight_weather_hits")
    out2 = out.assign(
        dt=out["ts"].dt.strftime("%Y-%m-%d"), hour=out["ts"].dt.strftime("%H")
    )
    write_parquet_partitioned(out2, out_dir, ["dt", "hour"])
    logger.info(
        "Joined %d hits → Postgres + Parquet. DB=%s", len(out), masked_dsn_for_log()
    )


if __name__ == "__main__":
    main()
