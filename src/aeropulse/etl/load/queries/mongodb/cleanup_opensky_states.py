# src/aeropulse/etl/load/queries/mongodb/cleanup_opensky_states.py

import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.load.loader.mongo_loader import get_collection

load_dotenv()
logger = setup_logger(__name__)

COLLECTION = "opensky_states_raw"


def main():
    days = int(os.getenv("OPENSKY_STATES_RETENTION_DAYS", "7"))
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    coll = get_collection(COLLECTION)
    res = coll.delete_many({"fetched_at": {"$lt": cutoff}})
    logger.info(
        "Deleted %d OpenSky raw documents older than %d days.", res.deleted_count, days
    )


if __name__ == "__main__":
    main()
