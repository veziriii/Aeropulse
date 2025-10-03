# src/aeropulse/etl/transform/queries/opensky_to_hits.py
import datetime as dt
from typing import Dict, Iterable, Iterator, List

import h3
from pymongo.collection import Collection
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _latest_region_docs(coll: Collection, limit_per_region: int = 1) -> List[Dict]:
    """Return the latest snapshot doc per region from Mongo."""
    out: List[Dict] = []
    for doc in coll.aggregate(
        [
            {"$sort": {"region_code": 1, "t": -1}},
            {"$group": {"_id": "$region_code", "doc": {"$first": "$$ROOT"}}},
        ]
    ):
        out.append(doc["doc"])
    return out


def _iter_states_from_docs(docs: Iterable[Dict]) -> Iterator[Dict]:
    """Yield individual state dicts from snapshot docs, carrying snapshot time."""
    for d in docs:
        t_snap = d.get("t")
        for s in d.get("states", []) or []:
            if s.get("latitude") is None or s.get("longitude") is None:
                continue
            yield {
                "t": t_snap,
                "icao24": s.get("icao24"),
                "callsign": (s.get("callsign") or "").strip() or None,
                "lat": float(s["latitude"]),
                "lon": float(s["longitude"]),
                "velocity": s.get("velocity"),
                "vertical_rate": s.get("vertical_rate"),
                "on_ground": s.get("on_ground"),
            }


def build_hits_from_latest_snapshots(
    *,
    mongo_opensky_coll: Collection,
    pg_engine: Engine,
    weather_staleness_minutes: int = 60,
) -> List[Dict]:
    """
    For latest OpenSky snapshots per region:
      - compute H3 res6 for each state
      - fetch the most recent curated weather for that cell (within staleness window)
      - return rows suitable for inserting into flight_weather_hits
    """
    docs = _latest_region_docs(mongo_opensky_coll)
    states = list(_iter_states_from_docs(docs))
    if not states:
        return []

    rows: List[Dict] = []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
        minutes=weather_staleness_minutes
    )

    needed_cells = sorted({h3.geo_to_h3(s["lat"], s["lon"], 6) for s in states})
    if not needed_cells:
        return []

    # fetch curated weather for those cells from Postgres
    sql = text(
        """
        SELECT h3_res6, last_updated, weather
        FROM public.weather_res6
        WHERE h3_res6 = ANY(:cells)
          AND last_updated IS NOT NULL
          AND last_updated >= :cutoff
        """
    )
    with pg_engine.begin() as conn:
        wrows = (
            conn.execute(sql, {"cells": needed_cells, "cutoff": cutoff})
            .mappings()
            .all()
        )

    weather_map = {
        r["h3_res6"]: {"last_updated": r["last_updated"], "weather": r["weather"]}
        for r in wrows
    }

    for s in states:
        cell = h3.geo_to_h3(s["lat"], s["lon"], 6)
        w = weather_map.get(cell)
        if not w:
            continue
        rows.append(
            {
                "icao24": s["icao24"],
                "callsign": s["callsign"],
                "t": s["t"],
                "h3_res6": cell,
                "weather_ts": w["last_updated"],
                "weather": w["weather"],
            }
        )
    return rows
