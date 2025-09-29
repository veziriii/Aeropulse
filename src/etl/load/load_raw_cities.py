import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path
from utils import setup_logger

load_dotenv()

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
logger = setup_logger("load_cities_to_mongodb.log")

client = MongoClient(os.getenv("MONGO_URI"))
db = client["Aeropulse"]
collection = db["cities"]


file_path = Path(DOWNLOAD_DIR)/"city_list_json.json"
with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

result = collection.insert_many(data)
count_in_db = collection.count_documents({})


if len(data) == count_in_db:
    logger.info("Data inserted successfully into MongoDB")
else:
    logger.error(
        f" Data insertion mismatch: file has {len(data)} records, "
        f"but collection has {count_in_db} records"
    )
    