import os
from datetime import datetime, timezone
import pandas as pd
import h3
from sqlalchemy import text
from aeropulse.etl.load.loader.mongo_loader import get_collection
from aeropulse.etl.load.loader.pg_loader import get_engine, masked_dsn_for_log
from aeropulse.utils.logging_config import setup_logger
from aeropulse.utils.parquet_io import write_parquet_partitioned

logger = setup_logger(__name__)


def _to_h3(lat, lon):
    if lat is None or lon is None:
        return None
    try:
        return h3.geo_to_h3(lat, lon, 6)
    except Exception:
        return None


def main():
    # raw collection name you already use for OpenSky snapshots:
    coll = get_collection("opensky_states")
    # last 20 minutes window to reduce load
    now = datetime.now(timezone.utc)
    since = now.timestamp() - 20 * 60

    cur = coll.find(
        {"time": {"$gte": since}},
        projection={"_id": 0, "time": 1, "states": 1},
        no_cursor_timeout=True,
    )

    rows = []
    for doc in cur:
        ts = datetime.fromtimestamp(doc["time"], tz=timezone.utc)
        for s in doc.get("states", []):
            # s is the array per OpenSky REST: [icao24, callsign, origin_country, time_position, last_contact, ...]
            icao24 = s[0]
            callsign = s[1]
            lat = s[6]
            lon = s[5]
            on_ground = s[8]
            velocity = s[9]
            heading = s[10]
            vert_rate = s[11]
            baro_altitude = s[7]
            geo_altitude = s[13] if len(s) > 13 else None
            cell = _to_h3(lat, lon)
            rows.append(
                {
                    "ts": ts,
                    "icao24": icao24,
                    "callsign": callsign,
                    "lat": lat,
                    "lon": lon,
                    "h3_res6": cell,
                    "on_ground": on_ground,
                    "velocity": velocity,
                    "heading": heading,
                    "vert_rate": vert_rate,
                    "geo_altitude": geo_altitude,
                    "baro_altitude": baro_altitude,
                }
            )

    if not rows:
        logger.info("No OpenSky rows to load.")
        return

    df = pd.DataFrame(rows)
    # write Parquet snapshot for offline viz
    out_dir = os.getenv("PROCESSED_DIR", "data/processed/opensky_states")
    write_parquet_partitioned(
        df.assign(dt=df["ts"].dt.strftime("%Y-%m-%d"), hour=df["ts"].dt.strftime("%H")),
        out_dir,
        ["dt", "hour"],
    )

    eng = get_engine()
    logger.info("Writing %d rows to Postgres (%s)", len(df), masked_dsn_for_log())
    # bulk insert via VALUES
    sql = text(
        """
        INSERT INTO public.opensky_states (
            ts, icao24, callsign, lat, lon, h3_res6, on_ground, velocity, heading, vert_rate, geo_altitude, baro_altitude
        )
        VALUES (
            :ts, :icao24, :callsign, :lat, :lon, :h3_res6, :on_ground, :velocity, :heading, :vert_rate, :geo_altitude, :baro_altitude
        )
    """
    )
    with eng.begin() as con:
        con.execute(sql, df.to_dict(orient="records"))
    logger.info("Done.")


if __name__ == "__main__":
    main()
