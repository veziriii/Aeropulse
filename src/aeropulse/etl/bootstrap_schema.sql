-- src/aeropulse/etl/bootstrap_schema.sql

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.cities_us (
  city_id  INTEGER PRIMARY KEY,
  name     TEXT,
  state    TEXT,
  country  TEXT,
  lat      DOUBLE PRECISION,
  lon      DOUBLE PRECISION
);
ALTER TABLE public.cities_us ADD COLUMN IF NOT EXISTS h3_res6 TEXT;
CREATE INDEX IF NOT EXISTS idx_cities_us_h3_res6 ON public.cities_us(h3_res6);

CREATE TABLE IF NOT EXISTS public.weather_res6 (
  h3_res6      TEXT PRIMARY KEY,
  last_updated TIMESTAMPTZ,
  weather      JSONB
);
CREATE INDEX IF NOT EXISTS idx_weather_res6_last_updated ON public.weather_res6(last_updated);

CREATE TABLE IF NOT EXISTS public.weather_res6_history (
  id      BIGSERIAL PRIMARY KEY,
  h3_res6 TEXT NOT NULL REFERENCES public.weather_res6(h3_res6) ON DELETE CASCADE,
  ts      TIMESTAMPTZ NOT NULL,
  weather JSONB
);
CREATE INDEX IF NOT EXISTS idx_weather_res6_history_h3_ts ON public.weather_res6_history (h3_res6, ts DESC);

CREATE TABLE IF NOT EXISTS public.flight_weather_hits (
  id              BIGSERIAL PRIMARY KEY,
  icao24          TEXT NOT NULL,
  call_sign       TEXT,
  ts              TIMESTAMPTZ NOT NULL,
  lat             DOUBLE PRECISION,
  lon             DOUBLE PRECISION,
  h3_res6         TEXT,
  weather_kind    TEXT,
  weather_temp_k  DOUBLE PRECISION,
  raw_weather     JSONB
);
CREATE INDEX IF NOT EXISTS idx_fwh_icao24_ts ON public.flight_weather_hits (icao24, ts);
CREATE INDEX IF NOT EXISTS idx_fwh_h3_ts    ON public.flight_weather_hits (h3_res6, ts);
