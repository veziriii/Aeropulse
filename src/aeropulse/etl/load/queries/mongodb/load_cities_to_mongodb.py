# src/aeropulse/etl/load/queries/mongodb/load_cities_to_mongodb.py

import os
from dotenv import load_dotenv

from aeropulse.etl.load.loader import (
    load_json_array_to_mongo,
    get_collection,
    create_indexes,
)


def _map_id_to__id(doc: dict) -> dict:
    """
    Ensure Mongo has a stable unique _id based on the OpenWeather city id.
    """
    if "id" in doc and "_id" not in doc:
        doc["_id"] = doc["id"]
    return doc


def main():
    load_dotenv()

    # Prefer explicit env var, fall back to the standard path in repo
    file_path = os.getenv(
        "CITY_LIST_JSON_PATH",
        os.path.join("data", "raw_data", "city_list_json.json"),
    )

    # If you keep only the .gz, point env var to it:
    # CITY_LIST_JSON_PATH=data/raw_data/city_list_json.gz

    inserted = load_json_array_to_mongo(
        file_path=file_path,
        collection_name="cities",
        batch_size=10000,
        drop_existing=True,
        transform=_map_id_to__id,  # <-- use transform instead of map_id_to__id
        indexes=[("country", 1), ("state", 1)],
    )

    # Just a tiny confirmation print; logs (if any) handled by your global config
    print(f"Inserted {inserted} documents into MongoDB.cities from {file_path}")


if __name__ == "__main__":
    main()
