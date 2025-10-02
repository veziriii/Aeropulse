from aeropulse.utils import setup_logger
from aeropulse.etl.transform.queries import us_cities_from_mongo
from aeropulse.etl.load.loader import ensure_table, bulk_upsert
import os

SCHEMA = os.getenv("PG_SCHEMA", "public")
TABLE = "cities_us"  # <- job-specific; no env

COLUMNS = {
    "city_id": "bigint primary key",
    "name": "text not null",
    "state": "text",
    "country": "char(2) not null",
    "lat": "double precision not null",
    "lon": "double precision not null",
}
INDEXES = [
    (f"idx_{TABLE}_name_state", ["name", "state"]),
    (f"idx_{TABLE}_geo", ["lat", "lon"]),
]


def main():
    logger = setup_logger("load_city_to_postgres.log")
    ensure_table(SCHEMA, TABLE, COLUMNS, INDEXES)
    total = 0
    for batch in us_cities_from_mongo(batch_size=5000):
        total += bulk_upsert(
            schema=SCHEMA,
            table=TABLE,
            rows=batch,
            conflict_cols=["city_id"],
            update_cols=["name", "state", "country", "lat", "lon"],
            chunk_size=10_000,
        )
        logger.info(f"Upserted {total} rows so far...")
    logger.info(f"Done. Total upserted: {total}")


if __name__ == "__main__":
    main()
