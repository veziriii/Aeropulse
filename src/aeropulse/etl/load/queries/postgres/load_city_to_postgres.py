# src/aeropulse/etl/load/queries/postgres/load_city_to_postgres.py

import os
from typing import Dict, Iterable, Iterator, List

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from aeropulse.etl.load.loader.mongo_loader import mongo_client
from aeropulse.etl.load.loader.pg_loader import get_engine, masked_dsn_for_log
from aeropulse.utils.logging_config import setup_logger

logger = setup_logger(__name__)


def _chunk(gen: Iterable[Dict], n: int) -> Iterator[List[Dict]]:
    batch: List[Dict] = []
    for x in gen:
        batch.append(x)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _mongo_us_cities(batch_size: int = 5000) -> Iterator[List[Dict]]:
    """
    Stream US cities from Mongo as batches with fields:
      city_id, name, state, country, lat, lon
    """
    client = mongo_client()
    dbname = os.getenv("MONGO_DB")
    if not dbname:
        raise RuntimeError("MONGO_DB not set")
    coll = client[dbname]["cities"]

    cursor = coll.find(
        {"country": "US", "coord.lat": {"$ne": None}, "coord.lon": {"$ne": None}},
        {
            "_id": 0,
            "id": 1,
            "name": 1,
            "state": 1,
            "coord.lat": 1,
            "coord.lon": 1,
        },
        no_cursor_timeout=True,
    ).batch_size(batch_size)

    def gen():
        for d in cursor:
            yield {
                "city_id": d["id"],
                "name": d.get("name"),
                "state": d.get("state"),
                "country": "US",
                "lat": d.get("coord", {}).get("lat"),
                "lon": d.get("coord", {}).get("lon"),
            }

    for b in _chunk(gen(), batch_size):
        yield b


def _upsert_batch(engine: Engine, rows: List[Dict]) -> int:
    """
    Upsert into public.cities_us:
      columns: city_id(pk), name, state, country, lat, lon
    """
    sql = text(
        """
        INSERT INTO public.cities_us (city_id, name, state, country, lat, lon)
        VALUES (:city_id, :name, :state, :country, :lat, :lon)
        ON CONFLICT (city_id) DO UPDATE SET
            name = EXCLUDED.name,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main():
    load_dotenv()

    # Connect to Postgres
    logger.info("Connecting to Postgres: %s", masked_dsn_for_log())
    engine = get_engine()

    total = 0
    for batch in _mongo_us_cities(batch_size=5000):
        total += _upsert_batch(engine, batch)
        logger.info("Upserted %d cities (cumulative: %d)", len(batch), total)

    logger.info("Finished upserting cities into Postgres. Total: %d", total)


if __name__ == "__main__":
    main()
