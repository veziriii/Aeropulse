# src/aeropulse/etl/pipelines/populate_flight_weather_hits.py
import os

from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from aeropulse.etl.transform.queries.opensky_to_hits import (
    build_hits_from_latest_snapshots,
)

load_dotenv()


def _mongo() -> MongoClient:
    uri = os.getenv("MONGO_URI") or "mongodb://%s:%s/" % (
        os.getenv("MONGO_HOST", "localhost"),
        int(os.getenv("MONGO_PORT", "27017")),
    )
    return MongoClient(uri)


def _engine():
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    return create_engine(dsn, future=True)


def _mask(dsn: str) -> str:
    return str(make_url(dsn).set(password="***"))


def main():
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    engine = _engine()
    mongo = _mongo()
    dbname = os.getenv("MONGO_DB")
    if not dbname:
        raise RuntimeError("MONGO_DB not set")
    coll = mongo[dbname]["opensky_states"]

    print(f"[hits] Postgres: {_mask(dsn)}")
    print(
        f"[hits] Reading latest OpenSky snapshots from MongoDB.{dbname}.opensky_states"
    )

    rows = build_hits_from_latest_snapshots(
        mongo_opensky_coll=coll,
        pg_engine=engine,
        weather_staleness_minutes=int(os.getenv("WEATHER_STALENESS_MINUTES", "60")),
    )
    if not rows:
        print("[hits] No joinable rows (no states or no fresh weather).")
        return

    # Upsert into flight_weather_hits on (icao24, t, h3_res6)
    sql = text(
        """
        INSERT INTO public.flight_weather_hits
            (icao24, callsign, t, h3_res6, weather_ts, weather)
        VALUES
            (:icao24, :callsign, :t, :h3_res6, :weather_ts, CAST(:weather AS JSONB))
        ON CONFLICT (icao24, t, h3_res6)
        DO UPDATE SET
            callsign = COALESCE(EXCLUDED.callsign, public.flight_weather_hits.callsign),
            weather_ts = EXCLUDED.weather_ts,
            weather = EXCLUDED.weather
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, rows)

    print(f"[hits] Upserted {len(rows)} flight-weather hit(s).")


if __name__ == "__main__":
    main()
