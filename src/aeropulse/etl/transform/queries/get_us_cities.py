import os
from typing import Dict, Iterable, Iterator, List, Optional
from dotenv import load_dotenv
from aeropulse.etl.load.loader import mongo_client

load_dotenv()


def _chunk(gen: Iterable[Dict], n: int) -> Iterator[List[Dict]]:
    """Yield fixed-size lists from an item generator.

    Args:
        gen: An iterable of dict-like rows.
        n: Target batch size (number of rows per yielded list).

    Yields:
        Lists of up to `n` dictionaries. The final batch may be smaller.
    """
    batch: List[Dict] = []
    for x in gen:
        batch.append(x)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def us_cities_from_mongo(
    batch_size: int = 5000,
    require_coords: bool = True,
) -> Iterator[List[Dict]]:
    """Stream US cities from MongoDB as Postgres-ready batches (no intermidiate files).

    Args:
        batch_size: Number of rows per yielded batch (also used as Mongo cursor batch size).
        require_coords: If True, skip documents missing `coord.lat` or `coord.lon`.

    Returns:
        An iterator over lists of dictionaries. Each dict has:
            - city_id (int): OpenWeather city ID.
            - name (str | None): City name.
            - state (str | None): Two-letter state code when present.
            - country (str): Always "US".
            - lat (float | None): Latitude.
            - lon (float | None): Longitude.
    """
    db = mongo_client()[os.getenv("MONGO_DB")]
    coll = db["cities"]

    query: Dict = {"country": "US"}

    projection = {
        "_id": 0,
        "id": 1,
        "name": 1,
        "state": 1,
        "coord.lat": 1,
        "coord.lon": 1,
    }

    cursor = coll.find(query, projection).batch_size(batch_size)

    gen = (
        {
            "city_id": d["id"],
            "name": d.get("name"),
            "state": d.get("state"),
            "country": "US",
            "lat": d.get("coord", {}).get("lat"),
            "lon": d.get("coord", {}).get("lon"),
        }
        for d in cursor
        if not require_coords
        or (
            d.get("coord", {}).get("lat") is not None
            and d.get("coord", {}).get("lon") is not None
        )
    )

    for batch in _chunk(gen, batch_size):
        yield batch
