import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# pull DSN from env
dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
if not dsn:
    raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")

engine = create_engine(dsn, future=True)

# make processed_data/run_id folder
run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
out_dir = os.path.join("processed_data", run_id)
os.makedirs(out_dir, exist_ok=True)


def export_table(query: str, name: str):
    """Helper: run query → dataframe → parquet"""
    df = pd.read_sql_query(text(query), engine)
    out_path = os.path.join(out_dir, f"{name}.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Exported {len(df)} rows to {out_path}")


def main():
    # 1. curated weather snapshot
    export_table("SELECT * FROM weather_res6", "weather_res6")

    # 2. optional history (latest 1 day)
    try:
        export_table(
            "SELECT * FROM weather_res6_history " "WHERE ts > NOW() - interval '1 day'",
            "weather_res6_history",
        )
    except Exception as e:
        print("Skip history:", e)

    # 3. optional flight hits (if table exists)
    try:
        export_table("SELECT * FROM flight_weather_hits", "flight_weather_hits")
    except Exception as e:
        print("Skip flight_weather_hits:", e)


if __name__ == "__main__":
    main()
