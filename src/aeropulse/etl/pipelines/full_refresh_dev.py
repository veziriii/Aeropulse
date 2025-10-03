# src/aeropulse/etl/pipelines/full_refresh_dev.py

import os
import subprocess
import sys


def run(cmd: list[str], env: dict | None = None):
    """Run a subprocess step and stream logs."""
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, env=env)


def main():
    # 1) Run Alembic migrations
    run([sys.executable, "-m", "alembic", "upgrade", "head"])

    # 2) Extract city list (optional, only if FORCE_EXTRACT set)
    force_extract = os.getenv("FORCE_EXTRACT", "false").lower() == "true"
    city_file = os.path.join("data", "raw_data", "city_list_json.json")
    if force_extract or not os.path.exists(city_file):
        run([sys.executable, "-m", "aeropulse.etl.extract.extract_city_list"])
    else:
        print(
            f"= city list present ({os.path.exists(city_file)}), "
            f"skip extract (FORCE_EXTRACT={force_extract})"
        )

    # 3) Raw cities → MongoDB
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.load_cities_to_mongodb",
        ]
    )

    # 4) Curated cities → Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_city_to_postgres",
        ]
    )

    # 5) Compute H3 indexes & seed weather cells in Postgres
    run([sys.executable, "-m", "aeropulse.etl.pipelines.populate_weather_cells"])

    # 6) Weather current → Mongo (dev batch size)
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

    # 7) Latest raw weather → curated snapshot in Postgres
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.postgres.load_weather_from_mongo_to_postgres",
        ]
    )

    # 8) Cleanup old raw weather docs in Mongo (optional in dev)
    run(
        [
            sys.executable,
            "-m",
            "aeropulse.etl.load.queries.mongodb.cleanup_weather_raw_mongodb",
        ]
    )

    # 9) 🔵 OpenSky: fetch live states over US tiles → Mongo (raw)
    #    Requires: OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET in your .env
    os_env = os.environ.copy()
    os_env["OPENSKY_MAX_TILES_PER_RUN"] = os_env.get("OPENSKY_MAX_TILES_PER_RUN", "6")
    os_env["OPENSKY_SLEEP_BETWEEN_CALLS"] = os_env.get(
        "OPENSKY_SLEEP_BETWEEN_CALLS", "0.5"
    )
    run(
        [sys.executable, "-m", "aeropulse.etl.extract.opensky.fetch_us_states"],
        env=os_env,
    )

    # 10) Optional: cleanup old OpenSky raw snapshots in Mongo
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
