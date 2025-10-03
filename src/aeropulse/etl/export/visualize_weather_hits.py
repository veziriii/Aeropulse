import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# find latest processed_data folder
folders = glob.glob("processed_data/*")
if not folders:
    raise RuntimeError(
        "No processed_data/* folder found. Run export_curated_to_parquet first."
    )

latest = max(folders, key=os.path.getmtime)
print("Using latest processed folder:", latest)

weather_path = os.path.join(latest, "weather_res6.parquet")
if not os.path.exists(weather_path):
    raise RuntimeError("weather_res6.parquet not found in latest run folder")

df = pd.read_parquet(weather_path)

# If JSON column 'weather' exists, flatten a few fields
if "weather" in df.columns:
    import json

    def parse_main(payload):
        try:
            obj = payload if isinstance(payload, dict) else json.loads(payload)
            if "weather" in obj and obj["weather"]:
                return obj["weather"][0].get("main")
        except Exception:
            return None

    df["wx_main"] = df["weather"].apply(parse_main)

# --- Simple plots ---
plt.figure(figsize=(8, 4))
df["wx_main"].value_counts().plot(kind="bar")
plt.title("Weather condition counts (latest snapshot)")
plt.tight_layout()
plt.show()

if "temp_k" in df.columns:
    plt.figure()
    df["temp_k"].hist(bins=40)
    plt.title("Temperature distribution (Kelvin)")
    plt.show()
