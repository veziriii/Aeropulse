# src/aeropulse/analytics/exports/hourly_hits_to_parquet.py
import os
import datetime as dt
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

load_dotenv()

DEF_WINDOW_MIN = int(os.getenv("HITS_EXPORT_WINDOW_MIN", "60"))
OUT_DIR = Path(os.getenv("PROCESSED_DIR", "data/processed")) / "flight_weather_hits"


def _engine():
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    return create_engine(dsn, future=True)


def main():
    eng = _engine()
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=DEF_WINDOW_MIN)

    sql = text(
        """
        SELECT icao24, callsign, t, h3_res6, weather_ts, weather
        FROM public.flight_weather_hits
        WHERE t >= :cutoff
        ORDER BY t
        """
    )

    with eng.begin() as conn:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff})

    if df.empty:
        print(f"[export] No hits in last {DEF_WINDOW_MIN} minutes.")
        return

    # partition by date/hour for easy querying later
    df["date"] = df["t"].dt.tz_convert("UTC").dt.date
    df["hour"] = df["t"].dt.tz_convert("UTC").dt.hour

    day = df["date"].iloc[0].strftime("%Y%m%d")
    hour = f"{int(df['hour'].iloc[0]):02d}"
    out_path = OUT_DIR / day / hour
    out_path.mkdir(parents=True, exist_ok=True)

    file_path = out_path / "hits.parquet"
    df.to_parquet(file_path, index=False)
    print(f"[export] Wrote {len(df)} rows → {file_path}")


if __name__ == "__main__":
    main()
