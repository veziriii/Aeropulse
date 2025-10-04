# Aeropulse

Aeropulse is a data engineering + analytics pipeline that integrates **flight tracking** (via OpenSky) with **weather data** (via OpenWeather) to study how atmospheric conditions affect flights in near-real time.  
It uses **PostgreSQL** and **MongoDB** for persistence, and supports exporting curated datasets into Parquet for analysis.

---

##  Project Structure

```
src/aeropulse
├── analytics/          # Data exports and visualization (plots, parquet outputs)
├── etl/                # Extract, Transform, Load pipelines
│   ├── extract/        # Data ingestion (cities, OpenSky API, weather API)
│   ├── load/           # Loaders into MongoDB & PostgreSQL
│   ├── transform/      # Transformations (H3 cells, joins, etc.)
│   └── pipelines/      # Orchestrated pipelines (full refresh, populate, etc.)
├── models/             # Database models (Postgres & MongoDB schemas)
├── services/           # API clients (OpenSky, OpenWeather)
├── utils/              # Logging, rate limiting, parquet IO
```

---

##  Getting Started

### 1. Install dependencies
Make sure you are inside a virtual environment, then:
```bash
pip install -e .
```

### 2. Configure environment
Define a `.env` file or export variables:
```bash
export POSTGRES_URI=postgresql+psycopg2://user:password@localhost:5432/Aeropulse
export MONGO_URI=mongodb://user:password@localhost:27017/Aeropulse
export OPENWEATHER_API_KEY=your_api_key
export WEATHER_UPDATE_BATCH=100
export WEATHER_MONGO_COLLECTION=weather_current_raw
```

### 3. Bootstrap the database
```bash
python -m aeropulse.etl.bootstrap_db
```

This will create the PostgreSQL schema defined in `bootstrap_schema.sql`.

---

## 🔄 Pipelines

### Full Refresh
Run the complete ETL pipeline:
```bash
python -m aeropulse.etl.pipelines.full_refresh_dev
```
This pipeline:
1. Loads US cities into Mongo & Postgres
2. Seeds H3 weather cells
3. Fetches and stores weather snapshots
4. Fetches OpenSky state vectors
5. Joins flights with weather observations

### Weather Cells
```bash
python -m aeropulse.etl.pipelines.populate_weather_cells
```

### Flight–Weather Hits
```bash
python -m aeropulse.etl.pipelines.populate_flight_weather_hits
```

---

## Analytics

- Export curated data to Parquet:
```bash
python -m aeropulse.etl.export.export_curated_to_parquet
```

- Plot last hour’s weather mix:
```bash
python -m aeropulse.analytics.plots.last_hour_weather_mix
```

---

##  Tech Stack

- **Databases**: PostgreSQL, MongoDB  
- **Data APIs**: OpenWeather, OpenSky  
- **Geo Indexing**: H3 hex cells  
- **Formats**: JSON, Parquet  
- **Orchestration**: Python subprocess pipelines  

---

##  Notes

- The pipeline is still under development.  
- Flight–weather joins depend on OpenSky API availability and can sometimes return empty results.  
- Some functions (e.g., H3) may require updating to match your installed version of `h3-py`.
