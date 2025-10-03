# src/aeropulse/etl/pipelines/full_refresh_dev.py

import os
import subprocess
import sys
from aeropulse.utils.logging_config import setup_logger
from aeropulse.etl.bootstrap_db import bootstrap_db

log = setup_logger(__name__)


def run(cmd: list[str], env: dict | None = None):
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, env=env)


def main():
    # 0) ensure DB schema is ready (no Alembic!)
    bootstrap_db()

    # 1) (optional) extract city list if missing
    city_json = os.path.join("data", "raw_data", "city_list_json.json")
    force = os.getenv("FORCE_EXTRACT", "false").lower() in {"1", "true", "yes"}
    if force or not os.path.exists(city_json):
        run([sys.executable, "-m", "aeropulse.etl.extract.extract_city_list"])
    else:
        print(
            f"= city list present ({os.path.exists(city_json)}), skip extract (FORCE_EXTRACT={force})"
        )

    # 2) raw cities -> Mongo
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_cities_to_mongodb",
        ]
    )

    # 3) curated cities -> Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_city_to_postgres",
        ]
    )

    # 4) compute H3 for cities & seed weather cells
    run([sys.executable, "-m", "aeropulse.etl.pipelines.populate_weather_cells"])

    # 5) fetch current weather raw into Mongo (small dev batch)
    env = os.environ.copy()
    env["WEATHER_UPDATE_BATCH"] = env.get("WEATHER_UPDATE_BATCH", "10")
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_weather_current_to_mongodb",
        ],
        env=env,
    )

    # 6) latest raw weather -> curated snapshot in Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_weather_from_mongo_to_postgres",
        ]
    )

    # 7) cleanup old weather raw in Mongo
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.cleanup_weather_raw_mongodb",
        ]
    )

    # 8) OpenSky states (by bounding boxes) -> Mongo
    run([sys.executable, "-m", "aeropulse.etl.extract.opensky.fetch_us_states"])

    # 9) cleanup old OpenSky raw in Mongo
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.cleanup_opensky_states",
        ]
    )

    print("\n full_refresh_dev completed successfully.")


if __name__ == "__main__":
    main()
