import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from aeropulse.etl.transform.queries.gen_h3_cells import compute_h3_res6
from aeropulse.etl.load.queries.postgres.load_weather_cells import insert_weather_cells


def main():
    db_url = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    engine = create_engine(db_url, future=True)
    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        cells = compute_h3_res6(session)
        inserted = insert_weather_cells(session, cells)
    print(f"Seeded {inserted} res6 weather cells")


if __name__ == "__main__":
    main()
