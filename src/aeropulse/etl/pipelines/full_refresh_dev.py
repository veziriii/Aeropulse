# src/aeropulse/etl/pipelines/full_refresh_dev.py

import os
import sys
import subprocess
from pathlib import Path


def run(cmd, env=None):
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, env=env)


def maybe_extract_city_list():
    """
    Extract fresh OpenWeather city list unless the file already exists,
    or FORCE_EXTRACT=1 is set in the environment.
    """
    force = os.getenv("FORCE_EXTRACT", "0") == "1"
    default_json = Path("data/raw_data/city_list_json.json")
    default_gz = Path("data/raw_data/city_list_json.gz")

    if force or (not default_json.exists() and not default_gz.exists()):
        run([sys.executable, "-m", "aeropulse.etl.extract.extract_city_list"])
    else:
        print(
            f"= city list present ({default_json.exists() or default_gz.exists()}), skip extract (FORCE_EXTRACT={force})"
        )


def main():
    # Make sure .env is loaded by subprocessed modules (they call load_dotenv themselves)
    # We only adjust a couple of environment defaults here for dev.
    env = os.environ.copy()
    env.setdefault("WEATHER_UPDATE_BATCH", "10")  # small bite for dev
    env.setdefault("WEATHER_FRESH_MINUTES", "30")

    # 1) Migrations
    run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env)

    # 2) Extract (skippable if file already exists)
    maybe_extract_city_list()

    # 3) Raw cities -> Mongo
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_cities_to_mongodb",
        ],
        env=env,
    )

    # 4) Curated cities -> Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_city_to_postgres",
        ],
        env=env,
    )

    # 5) Compute H3 for cities + seed weather cells table
    run(
        [sys.executable, "-m", "aeropulse.etl.pipelines.populate_weather_cells"],
        env=env,
    )

    # 6) Fetch current weather for stale cells -> Mongo (raw, budgeted)
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_weather_current_to_mongodb",
        ],
        env=env,
    )

    # 7) Latest raw -> Postgres curated snapshot
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_weather_from_mongo_to_postgres",
        ],
        env=env,
    )

    # 8) Cleanup old raw in Mongo (optional but cheap; controlled by retention env)
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.cleanup_weather_raw_mongodb",
        ],
        env=env,
    )

    print("\n full_refresh_dev completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(
            f"\n  Step failed with exit code {e.returncode}: {' '.join(e.cmd)}",
            flush=True,
        )
        sys.exit(e.returncode)
