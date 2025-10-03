# src/aeropulse/etl/extract/opensky/fetch_us_states.py

import os
import time
from datetime import datetime, timezone
from typing import Dict, List

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.load.loader.mongo_loader import (
    get_collection,
    create_indexes,
    insert_batch,
)
from aeropulse.services.opensky_client import get_states_all

load_dotenv()
logger = setup_logger(__name__)

COLLECTION = "opensky_states_raw"

# Rough tiling of CONUS; tweak as needed
US_TILES: List[Dict] = [
    {"bbox_id": "W1", "lamin": 32.0, "lomin": -125.0, "lamax": 42.0, "lomax": -114.0},
    {"bbox_id": "W2", "lamin": 42.0, "lomin": -125.0, "lamax": 49.5, "lomax": -114.0},
    {"bbox_id": "SW", "lamin": 24.0, "lomin": -117.0, "lamax": 32.0, "lomax": -106.0},
    {"bbox_id": "C1", "lamin": 32.0, "lomin": -114.0, "lamax": 42.0, "lomax": -103.0},
    {"bbox_id": "C2", "lamin": 42.0, "lomin": -114.0, "lamax": 49.5, "lomax": -103.0},
    {"bbox_id": "SC", "lamin": 24.0, "lomin": -106.0, "lamax": 32.0, "lomax": -95.0},
    {"bbox_id": "E1", "lamin": 32.0, "lomin": -103.0, "lamax": 42.0, "lomax": -90.0},
    {"bbox_id": "E2", "lamin": 42.0, "lomin": -103.0, "lamax": 49.5, "lomax": -90.0},
    {"bbox_id": "SE", "lamin": 24.0, "lomin": -95.0, "lamax": 32.0, "lomax": -80.0},
    {"bbox_id": "E3", "lamin": 32.0, "lomin": -90.0, "lamax": 42.0, "lomax": -80.0},
    {"bbox_id": "E4", "lamin": 42.0, "lomin": -90.0, "lamax": 49.5, "lomax": -66.5},
    {"bbox_id": "SE2", "lamin": 24.0, "lomin": -80.0, "lamax": 32.0, "lomax": -66.5},
]


def _ensure_indexes():
    coll = get_collection(COLLECTION)
    create_indexes(
        coll,
        [
            ("fetched_at", DESCENDING),
            ("time", DESCENDING),  # OpenSky server epoch for the snapshot
            ("bbox_id", ASCENDING),
        ],
        background=True,
    )


def main():
    _ensure_indexes()

    max_tiles = int(os.getenv("OPENSKY_MAX_TILES_PER_RUN", "6"))
    sleep_sec = float(os.getenv("OPENSKY_SLEEP_BETWEEN_CALLS", "0.5"))

    fetched_at = datetime.now(tz=timezone.utc)
    total = 0

    for i, tile in enumerate(US_TILES):
        if i >= max_tiles:
            break
        try:
            data = get_states_all(
                lamin=tile["lamin"],
                lomin=tile["lomin"],
                lamax=tile["lamax"],
                lomax=tile["lomax"],
                extended=True,
            )
        except Exception as e:
            logger.warning("OpenSky error for %s: %s", tile["bbox_id"], e)
            time.sleep(sleep_sec)
            continue

        doc = {"bbox_id": tile["bbox_id"], "fetched_at": fetched_at, **data}
        total += insert_batch(get_collection(COLLECTION), [doc])

        logger.info(
            "Fetched states for %s: t=%s, rows=%s",
            tile["bbox_id"],
            data.get("time"),
            len(data.get("states") or []),
        )
        time.sleep(sleep_sec)

    logger.info("Inserted %d OpenSky state snapshots into Mongo.", total)


if __name__ == "__main__":
    main()
