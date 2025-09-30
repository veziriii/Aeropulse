import os
import json
from pathlib import Path
from typing import List, Dict
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from aeropulse.utils import setup_logger

logger = setup_logger("mongo_loader.log")


def mongo_client() -> MongoClient:

    load_dotenv()
    uri = os.getenv("MONGO_URI")
    if uri:
        return MongoClient(uri)
    else:
        raise ValueError("MONGO_URI is not set in .env")


def load_json_array_to_mongo(
    file_path: str | Path,
    collection_name: str,
    id_field: str,
    map_id_to__id: bool = True,
    chunk_size: int = 10_000,
    continue_on_error: bool = True,
) -> dict:
    """
    Load a JSON array file into MongoDB with bulk writes.

    If `map_id_to__id=True`, a non-empty `id_field` (default "id") is
    used as `_id` for idempotent loads; otherwise Mongo generates `_id`.
    """

    file_path = Path(file_path)
    client = mongo_client()

    db_name = os.getenv("MONGO_DB")
    if not db_name:
        raise ValueError("db_name not provided and MONGO_DB not set in .env")

    col = client[db_name][collection_name]
    logger.info(f"Loading {file_path} into {db_name}.{collection_name}")

    with open(file_path, "r", encoding="utf-8") as f:
        data: List[Dict] = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Top-level JSON must be an array.")

    total_read = 0
    total_inserted = 0
    total_duplicates = 0
    total_failed = 0

    def write_batch(batch_ops: List[InsertOne]):
        nonlocal total_inserted, total_duplicates, total_failed
        if not batch_ops:
            return
        try:
            res = col.bulk_write(batch_ops, ordered=False)
            total_inserted += res.inserted_count or 0
        except BulkWriteError as bwe:
            errs = bwe.details.get("writeErrors", [])
            dup = sum(1 for e in errs if e.get("code") == 11000)
            other = len(errs) - dup
            total_duplicates += dup
            total_failed += other
            if other and not continue_on_error:
                raise
            if other:
                logger.warning(f"{other} non-duplicate errors in a chunk")

    batch_ops: List[InsertOne] = []
    for doc in data:
        total_read += 1

        if map_id_to__id and "_id" not in doc:
            if id_field in doc:
                raw_id = doc[id_field]
                if isinstance(raw_id, str):
                    if raw_id.strip():
                        doc["_id"] = raw_id
                elif raw_id is not None:
                    doc["_id"] = raw_id

        batch_ops.append(InsertOne(doc))
        if len(batch_ops) >= chunk_size:
            write_batch(batch_ops)
            batch_ops = []

    write_batch(batch_ops)

    count_now = col.count_documents({})
    summary = {
        "read": total_read,
        "inserted": total_inserted,
        "duplicates": total_duplicates,
        "other_errors": total_failed,
        "collection_count_now": count_now,
        "collection": f"{db_name}.{collection_name}",
        "file": str(file_path),
    }
    logger.info(f"Summary: {summary}")
    return summary
