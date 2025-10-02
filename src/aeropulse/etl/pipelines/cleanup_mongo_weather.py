import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from aeropulse.utils.logging_config import setup_logger

load_dotenv()
logger = setup_logger(__name__)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "Aeropulse")
COLL_NAME = "weather_current_raw"

# Retention (days) from .env, default = 30 days
RETENTION_DAYS = int(os.getenv("WEATHER_MONGO_RETENTION_DAYS", "30"))


def cleanup_old_weather():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLL_NAME]

    # ensure index for fetched_at to speed up deletes
    coll.create_index([("fetched_at", ASCENDING)])

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    logger.info("Deleting docs older than %s (cutoff)", cutoff.isoformat())

    result = coll.delete_many({"fetched_at": {"$lt": cutoff}})
    logger.info(
        "Deleted %d old documents from %s.%s", result.deleted_count, DB_NAME, COLL_NAME
    )


def main():
    cleanup_old_weather()


if __name__ == "__main__":
    main()
