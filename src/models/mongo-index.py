import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from utils import setup_logger 

logger = setup_logger("mongo_index.log")

load_dotenv()

def setup_indexes(uri= os.getenv("MONGO_URI"), db_name = os.getenv("MONGO_DB")):
    client = MongoClient(uri)
    col = client[db_name]["cities"]

    col.create_index([("country", ASCENDING)], name="idx_country")
    logger.info(f"'contry' has been indexed in cities collection")

if __name__ == "__main__":
    setup_indexes()
