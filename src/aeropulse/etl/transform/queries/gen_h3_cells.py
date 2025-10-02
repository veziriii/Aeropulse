import math
import h3
from typing import List, Set
from sqlalchemy.orm import Session
from sqlalchemy import select
from aeropulse.models.city import City
from aeropulse.utils.logging_config import setup_logger  # or your get_logger alias

logger = setup_logger(__name__)


# Compatibility wrapper for H3 v3/v4
def _h3_index(lat: float, lon: float, res: int) -> str:
    # prefer v3 name if present
    if hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lon, res)
    # v4 name
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    raise RuntimeError("No suitable H3 function found (geo_to_h3 or latlng_to_cell).")


def _coerce_finite(x):
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def compute_h3_res6(session: Session) -> List[str]:
    # If your table is big, consider chunking with .offset/.limit like before.
    rows = session.execute(select(City)).scalars().all()

    unique: Set[str] = set()
    skipped = 0
    for city in rows:
        lat = _coerce_finite(city.lat)
        lon = _coerce_finite(city.lon)
        if (
            lat is None
            or lon is None
            or not (-90 <= lat <= 90)
            or not (-180 <= lon <= 180)
        ):
            skipped += 1
            continue
        try:
            cell = _h3_index(lat, lon, 6)
        except Exception as e:
            # log and skip this row
            logger.warning(
                "Failed H3 for city_id=%s (%s,%s): %s",
                getattr(city, "city_id", None),
                lat,
                lon,
                e,
            )
            skipped += 1
            continue
        city.h3_res6 = cell
        unique.add(cell)

    session.commit()
    logger.info(
        "Updated %d cities (skipped %d). Unique res6 cells: %d",
        len(rows) - skipped,
        skipped,
        len(unique),
    )
    return list(unique)
