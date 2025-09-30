# pg_loader.py
import os
from typing import Dict, List, Iterable, Tuple, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


def _pg_dsn() -> str:
    """Return a SQLAlchemy DSN from env (POSTGRES_DSN or DATABASE_URL)."""
    dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set POSTGRES_DSN or DATABASE_URL in your .env")
    return dsn


def get_engine() -> Engine:
    """Create a Postgres engine with safe defaults."""
    # pool_pre_ping helps when connections sit idle
    return create_engine(_pg_dsn(), pool_pre_ping=True)  # add future=True if on SA 1.4


def ensure_table(
    schema: str,
    table: str,
    columns: Dict[str, str],
    indexes: Optional[List[Tuple[str, List[str]]]] = None,
) -> None:
    """Create schema/table (and indexes) if they do not already exist."""
    indexes = indexes or []
    cols_sql = ",\n    ".join(f"{c} {t}" for c, t in columns.items())

    eng = get_engine()
    with eng.begin() as cx:
        # One statement per execute; wrap in text(...) for SA 2.x
        cx.execute(text(f"create schema if not exists {schema}"))
        cx.execute(text(f"create table if not exists {schema}.{table} (\n    {cols_sql}\n)"))
        for idx_name, cols in indexes:
            cx.execute(text(
                f"create index if not exists {idx_name} "
                f"on {schema}.{table}({', '.join(cols)})"
            ))


def bulk_upsert(
    schema: str,
    table: str,
    rows: Iterable[Dict],
    conflict_cols: List[str],
    update_cols: Optional[List[str]] = None,
    chunk_size: int = 10_000,
) -> int:
    """Bulk UPSERT rows into Postgres via INSERT ... ON CONFLICT ... DO UPDATE.

    Args:
        schema: Target schema (e.g., "public").
        table: Target table (e.g., "cities_us").
        rows: Iterable of dicts whose keys match table columns.
        conflict_cols: Columns that define uniqueness (e.g., ["city_id"] or ["city_id","ts"]).
        update_cols: Columns to update on conflict; defaults to all non-conflict cols.
        chunk_size: Number of rows per DB roundtrip.

    Returns:
        Total number of rows processed (inserted or updated).
    """
    rows = list(rows)
    if not rows:
        return 0

    # Infer column order from the first row
    cols = list(rows[0].keys())

    # (Optional) quick consistency check on first few rows
    for r in rows[1:50]:
        if list(r.keys()) != cols:
            raise ValueError("All rows must have the same columns/order.")

    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict_cols]

    col_list = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    conflict = ", ".join(conflict_cols)
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols) or "/* no updates */"

    sql = text(f"""
        insert into {schema}.{table} ({col_list})
        values ({placeholders})
        on conflict ({conflict}) do update
        set {set_clause}
    """)

    total = 0
    eng = get_engine()
    with eng.begin() as cx:
        for i in range(0, len(rows), chunk_size):
            batch = rows[i:i + chunk_size]
            cx.execute(sql, batch)  # SQLA expands list-of-dicts efficiently
            total += len(batch)
    return total

