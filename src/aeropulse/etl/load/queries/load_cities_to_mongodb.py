import os
from pathlib import Path
from dotenv import load_dotenv
from aeropulse.etl.load.loader import load_json_array_to_mongo

load_dotenv()

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")

if not DOWNLOAD_DIR:
    raise RuntimeError("Set DOWNLOAD_DIR in your .env")

file_path = Path(DOWNLOAD_DIR) / "city_list_json.json"

if not file_path.exists():
    raise FileNotFoundError(f"File not found: {file_path}")


if __name__ == "__main__":

    load_json_array_to_mongo(
        file_path=file_path,
        collection_name="cities",
        map_id_to__id=True,
        id_field="id",
        chunk_size=10_000,
        continue_on_error=True,
    )
