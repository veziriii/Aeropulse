import os
import logging
from typing import Set, List

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url

from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.transform.queries.gen_h3_cells import compute_h3_res6

logger = setup_logger(__name__)
# keep HTTP noise quiet just in case
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def main():
    load_dotenv()

    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")

    # Mask password in DSN logs
    safe_url = make_url(dsn).set(password="***")
    logger.info("Connecting to DB: %s", safe_url)

    engine = create_engine(dsn, future=True)
    Session = sessionmaker(bind=engine, future=True)

    # 1) Compute & set h3_res6 on cities_us, collect unique cells
    with Session() as session:
        unique_cells: List[str] = compute_h3_res6(session)
    logger.info("Collected %d unique res6 cells from cities.", len(unique_cells))

    if not unique_cells:
        logger.info("No H3 cells found; nothing to seed.")
        return

    # 2) Seed weather_res6 (insert missing cells only)
    # Using a single statement with unnest to be efficient
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                WITH new_cells AS (
                    SELECT UNNEST(:cells::text[]) AS h3
                )
                INSERT INTO public.weather_res6 (h3_res6, last_updated, weather)
                SELECT nc.h3, NULL, NULL
                FROM new_cells nc
                LEFT JOIN public.weather_res6 w ON w.h3_res6 = nc.h3
                WHERE w.h3_res6 IS NULL
            """
            ),
            {"cells": unique_cells},
        )

    logger.info("Seeding done.")


if __name__ == "__main__":
    main()
