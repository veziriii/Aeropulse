# src/aeropulse/etl/pipelines/populate_weather_cells.py

import os
from typing import List, Set

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.transform.queries.gen_h3_cells import compute_h3_res6

logger = setup_logger(__name__)


def main():
    load_dotenv()

    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")

    logger.info("Connecting to DB: %s", make_url(dsn).set(password="***"))
    engine = create_engine(dsn, future=True)

    # 1) compute h3_res6 for cities (updates cities_us.h3_res6) and collect unique cells
    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        cells_set: Set[str] = compute_h3_res6(
            session
        )  # updates cities_us and returns unique cells
    cells: List[str] = sorted(cells_set)
    logger.info("Collected %d unique res6 cells from cities.", len(cells))

    if not cells:
        return

    # 2) seed weather_res6 with those cells; use batched INSERT ... ON CONFLICT DO NOTHING
    #    (avoids UNNEST/ARRAY param casting issues)
    insert_sql = text(
        """
        INSERT INTO public.weather_res6 (h3_res6, last_updated, weather)
        VALUES (:h3, NULL, NULL)
        ON CONFLICT (h3_res6) DO NOTHING
        """
    )

    batch_size = 1000
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(cells), batch_size):
            batch = cells[i : i + batch_size]
            params = [{"h3": c} for c in batch]
            conn.execute(insert_sql, params)
            total += len(batch)

    logger.info("Seeded %d res6 weather cells", total)


if __name__ == "__main__":
    main()
