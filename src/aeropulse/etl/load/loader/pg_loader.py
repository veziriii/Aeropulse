# src/aeropulse/etl/load/loader/pg_loader.py

import os
from typing import Iterable, Mapping, Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

load_dotenv()


def get_engine() -> Engine:
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL")
    return create_engine(dsn, future=True)


def masked_dsn_for_log() -> str:
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    return str(make_url(dsn).set(password="***")) if dsn else "<unset>"


def upsert_jsonb_rows(
    *,
    table: str,
    pk_column: str,
    jsonb_column: str,
    ts_column: str,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    """
    Upsert rows into a table with a JSONB payload and timestamp.
    Expects each row to have keys: 'pk', 'payload', 'ts'.
    INSERT ... ON CONFLICT (pk) DO UPDATE ...
    """
    payload = []
    for r in rows:
        pk = r.get("pk")
        data = r.get("payload")
        ts = r.get("ts")
        if pk is None or data is None or ts is None:
            continue
        payload.append({"pk": pk, "payload": data, "ts": ts})

    if not payload:
        return 0

    eng = get_engine()
    sql = text(
        f"""
        INSERT INTO {table} ({pk_column}, {jsonb_column}, {ts_column})
        VALUES (:pk, CAST(:payload AS JSONB), :ts)
        ON CONFLICT ({pk_column})
        DO UPDATE SET
            {jsonb_column} = EXCLUDED.{jsonb_column},
            {ts_column} = EXCLUDED.{ts_column}
    """
    )

    with eng.begin() as conn:
        conn.execute(sql, payload)

    return len(payload)
