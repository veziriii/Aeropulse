import os
import json
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path


load_dotenv()


LOG_DIR = os.getenv("LOG_DIR")  
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)  
LOG_FILE = Path(LOG_DIR) / "city_list_to_mongo.log"
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")


logging.basicConfig(
    level=20,
    filename=LOG_FILE,
    encoding="utf-8",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)

client = MongoClient(os.getenv("MONGO_URI"))
db = client["Aeropulse"]
collection = db["cities"]


file_path = Path(DOWNLOAD_DIR)/"city_list_json.json"
with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

result = collection.insert_many(data)
count_in_db = collection.count_documents({})


if len(data) == count_in_db:
    logging.info("Data inserted successfully into MongoDB")
else:
    logging.error(
        f" Data insertion mismatch: file has {len(data)} records, "
        f"but collection has {count_in_db} records"
    )
    