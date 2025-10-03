# src/aeropulse/etl/pipelines/full_refresh_dev.py

import os
import sys
import subprocess
from datetime import datetime

from aeropulse.etl.bootstrap_db import bootstrap_db


def run(cmd, env=None):
    """Run a python -m step and echo the command."""
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, env=env)


def city_list_present():
    path_json = os.path.join("data", "raw_data", "city_list_json.json")
    path_gz = os.path.join("data", "raw_data", "city_list_json.gz")
    return os.path.exists(path_json) or os.path.exists(path_gz)


def main():
    # 0) Bootstrap DB schema (idempotent)
    print("= bootstrapping database schema (no Alembic)...", flush=True)
    bootstrap_db()

    # 1) Optionally extract city list (skip if file already exists unless forced)
    force_extract = os.getenv("FORCE_EXTRACT", "false").lower() in ("1", "true", "yes")
    if force_extract or not city_list_present():
        print("= extracting city list from OpenWeather...", flush=True)
        run([sys.executable, "-m", "aeropulse.etl.extract.extract_city_list"])
    else:
        print(
            f"= city list present ({city_list_present()}), skip extract (FORCE_EXTRACT={force_extract})"
        )

    # 2) Raw cities -> MongoDB
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_cities_to_mongodb",
        ]
    )

    # 3) Curated cities -> Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_city_to_postgres",
        ]
    )

    # 4) Compute H3 cells from cities and seed weather cells in Postgres
    run([sys.executable, "-m", "aeropulse.etl.pipelines.populate_weather_cells"])

    # 5) Current weather for a small batch of cells -> Mongo raw
    #    You can control batch via WEATHER_UPDATE_BATCH env (default inside module)
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_weather_current_to_mongodb",
        ]
    )

    # 6) Latest raw weather (Mongo) -> curated snapshot in Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_weather_from_mongo_to_postgres",
        ]
    )

    # 7) Cleanup old raw weather in Mongo (retention via WEATHER_MONGO_RETENTION_DAYS)
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.cleanup_weather_raw_mongodb",
        ]
    )

    # 8) OpenSky: fetch US states into Mongo raw (requires OPENSKY_* env set)
    run([sys.executable, "-m", "aeropulse.etl.extract.opensky.fetch_us_states"])

    # 9) Load OpenSky states (Mongo) -> Postgres history table
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_opensky_states_from_mongo",
        ]
    )

    # 10) Build hourly flight↔weather hits in Postgres
    run([sys.executable, "-m", "aeropulse.etl.pipelines.populate_flight_weather_hits"])

    # (Optional) Export curated analytics to parquet / plots
    # run([sys.executable, "-m", "aeropulse.analytics.exports.hourly_hits_to_parquet"])
    # run([sys.executable, "-m", "aeropulse.analytics.plots.last_hour_weather_mix"])

    print(
        "\n full_refresh_dev completed successfully at",
        datetime.utcnow().isoformat(),
        "UTC",
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print("\n  Step failed with exit code", e.returncode, ":", " ".join(e.cmd))
        sys.exit(1)
