import os
from aeropulse.utils import setup_logger
from aeropulse.etl.load.loader import mongo_client


def main():
    load_dotenv()
    logger = setup_logger("mongo_index.log")

    db_name = os.getenv("MONGO_DB")
    if not db_name:
        raise RuntimeError("MONGO_DB is not set in .env")

    client = mongo_client()
    db = client[db_name]
    c = db["cities"]

    c.create_index([("country", 1)], name="idx_country")
    logger.info("Ensured index: idx_country on ('country', 1)")

    c.create_index([("coord.lat", 1), ("coord.lon", 1)], name="idx_coord_lat_lon")
    logger.info(
        "Ensured index: idx_coord_lat_lon on ('coord.lat', 1), ('coord.lon', 1)"
    )

    logger.info("Mongo indexes ensured successfully.")


if __name__ == "__main__":
    main()
