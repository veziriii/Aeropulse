import os
import json
import gzip
from typing import (
    Iterable,
    List,
    Dict,
    Any,
    Sequence,
    Mapping,
    Optional,
    Callable,
    Iterator,
)

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection

load_dotenv()

# ---- Core connection helper (kept simple & stable) -------------------------


def mongo_client() -> MongoClient:
    """
    Build a MongoClient from env:
      - MONGO_URI (preferred), e.g. mongodb://user:pass@localhost:27017/Aeropulse
      - or MONGO_HOST/MONGO_PORT (+ optional MONGO_USER/MONGO_PASS)
    """
    uri = os.getenv("MONGO_URI")
    if uri:
        return MongoClient(uri)

    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", "27017"))
    user = os.getenv("MONGO_USER")
    pwd = os.getenv("MONGO_PASS")

    if user and pwd:
        uri = f"mongodb://{user}:{pwd}@{host}:{port}/"
        return MongoClient(uri)
    return MongoClient(host=host, port=port)


def _get_db_name() -> str:
    name = os.getenv("MONGO_DB")
    if not name:
        raise RuntimeError("MONGO_DB not set")
    return name


def get_collection(name: str) -> Collection:
    """Return a handle to a collection under the default DB."""
    db = mongo_client()[_get_db_name()]
    return db[name]


# ---- Utility helpers used by pipelines -------------------------------------


def create_indexes(
    collection: Collection,
    specs: Sequence[tuple[str, int]],
    background: bool = True,
) -> None:
    """
    Ensure simple single-field indexes exist (idempotent).
    Example: create_indexes(coll, [("h3_res6", 1), ("fetched_at", -1)])
    """
    for field, order in specs:
        collection.create_index([(field, order)], background=background)


def insert_batch(collection: Collection, docs: Iterable[Mapping[str, Any]]) -> int:
    """
    Insert a batch of documents (skips if empty). Returns inserted count.
    """
    docs_list = list(docs)
    if not docs_list:
        return 0
    result = collection.insert_many(docs_list)
    return len(result.inserted_ids)


def latest_docs_by_keys(
    collection: str | Collection,
    *,
    key_field: str,
    keys: Sequence[Any],
    sort_field: str = "fetched_at",
) -> Dict[Any, Dict[str, Any]]:
    """
    For a set of keys, return the most recent document per key (by sort_field).
    Uses an aggregation: $match → $sort → $group(first).
    """
    coll = (
        collection if isinstance(collection, Collection) else get_collection(collection)
    )
    if not keys:
        return {}

    pipeline = [
        {"$match": {key_field: {"$in": list(keys)}}},
        {"$sort": {key_field: ASCENDING, sort_field: DESCENDING}},
        {"$group": {"_id": f"${key_field}", "doc": {"$first": "$$ROOT"}}},
    ]

    out: Dict[Any, Dict[str, Any]] = {}
    for row in coll.aggregate(pipeline, allowDiskUse=True):
        out[row["_id"]] = row["doc"]
    return out


# ---- JSON array file → Mongo (no ijson) ------------------------------------


def _iter_json_array(file_path: str) -> Iterator[Dict[str, Any]]:
    """
    Yield objects from a JSON array file by fully loading it into memory.
    Supports .gz files by extension.
    """
    use_gzip = file_path.endswith(".gz")
    open_fn = gzip.open if use_gzip else open

    mode = "rt" if use_gzip else "r"
    with open_fn(file_path, mode, encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Expected a top-level JSON array")
        for obj in data:
            if isinstance(obj, dict):
                yield obj


def load_json_array_to_mongo(
    *,
    file_path: str,
    collection_name: str,
    batch_size: int = 10000,
    drop_existing: bool = False,
    transform: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    indexes: Optional[Sequence[tuple[str, int]]] = None,
) -> int:
    """
    Load a JSON array file (optionally .gz) into a Mongo collection in batches.

    Args:
        file_path: path to JSON file (array) or gzipped JSON (.gz).
        collection_name: Mongo collection name to load into.
        batch_size: number of docs per insert_many.
        drop_existing: if True, drops the collection first.
        transform: optional function(doc) -> new_doc or None (to filter).
        indexes: optional list of (field, order) to ensure after load.

    Returns:
        Total number of inserted documents.
    """
    coll = get_collection(collection_name)
    if drop_existing:
        coll.drop()

    total = 0
    batch: List[Dict[str, Any]] = []

    for doc in _iter_json_array(file_path):
        if transform is not None:
            doc = transform(doc)
            if doc is None:
                continue
        batch.append(doc)
        if len(batch) >= batch_size:
            total += insert_batch(coll, batch)
            batch = []

    if batch:
        total += insert_batch(coll, batch)

    if indexes:
        create_indexes(coll, indexes)

    return total
