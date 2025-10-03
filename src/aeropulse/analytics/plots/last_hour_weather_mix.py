# src/aeropulse/analytics/plots/last_hour_weather_mix.py
import os
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

WINDOW_MIN = int(os.getenv("HITS_PLOT_WINDOW_MIN", "60"))
PLOTS_DIR = Path(os.getenv("PROCESSED_DIR", "data/processed")) / "plots"


def _engine():
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    return create_engine(dsn, future=True)


def _weather_main(w: Dict[str, Any]) -> str:
    """
    Extract a 'main' label for quick grouping.
    We try OpenWeather-like formats:
      - if payload has 'weather' array with dicts: take weather[0]['main']
      - else fallback to 'Unknown'
    """
    if not isinstance(w, dict):
        return "Unknown"
    arr = w.get("weather")
    if isinstance(arr, list) and arr:
        first = arr[0] or {}
        label = first.get("main")
        if isinstance(label, str) and label:
            return label
    return "Unknown"


def main():
    eng = _engine()
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=WINDOW_MIN)

    sql = text(
        """
        SELECT icao24, callsign, t, h3_res6, weather
        FROM public.flight_weather_hits
        WHERE t >= :cutoff
        """
    )
    with eng.begin() as conn:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff})

    if df.empty:
        print(f"[plots] No hits in last {WINDOW_MIN} minutes.")
        return

    # Extract a simple label for weather
    df["weather_main"] = df["weather"].apply(_weather_main)

    # 1) pie: mix of weather_main
    counts = df["weather_main"].value_counts().sort_values(ascending=False)

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")

    # pie
    plt.figure()
    counts.plot(kind="pie", autopct="%1.1f%%")
    plt.title(f"Weather mix (last {WINDOW_MIN} min) — n={len(df)}")
    plt.ylabel("")
    out1 = PLOTS_DIR / f"weather_mix_{ts}.png"
    plt.savefig(out1, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"[plots] saved {out1}")

    # 2) bar: top 15 callsigns by hits
    top_callsigns = (
        df.assign(callsign=df["callsign"].fillna("UNKNOWN"))
        .groupby("callsign", dropna=False)
        .size()
        .sort_values(ascending=False)
        .head(15)
    )
    plt.figure()
    top_callsigns.plot(kind="bar")
    plt.title(f"Top callsigns by hits (last {WINDOW_MIN} min)")
    plt.xlabel("callsign")
    plt.ylabel("hits")
    plt.xticks(rotation=45, ha="right")
    out2 = PLOTS_DIR / f"top_callsigns_{ts}.png"
    plt.savefig(out2, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"[plots] saved {out2}")


if __name__ == "__main__":
    main()
