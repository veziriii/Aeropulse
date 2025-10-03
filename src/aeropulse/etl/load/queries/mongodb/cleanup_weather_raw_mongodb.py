import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from aeropulse.etl.load.loader.mongo_loader import get_collection
from aeropulse.utils.logging_config import setup_logger

logger = setup_logger(__name__)


def main():
    load_dotenv()
    days = int(os.getenv("WEATHER_MONGO_RETENTION_DAYS", "30"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    coll = get_collection("weather_current_raw")
    res = coll.delete_many({"fetched_at": {"$lt": cutoff}})
    logger.info(
        "Deleted %d old weather docs (older than %d days).", res.deleted_count, days
    )


if __name__ == "__main__":
    main()
