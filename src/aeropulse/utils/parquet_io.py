import os
from pathlib import Path
from typing import Optional
import pandas as pd


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_parquet(
    df: pd.DataFrame, base_dir: str | Path, filename: Optional[str] = None
) -> str:
    ensure_dir(base_dir)
    if filename is None:
        filename = "part.parquet"
    out = Path(base_dir) / filename
    df.to_parquet(out, index=False)
    return str(out)


def write_parquet_partitioned(
    df: pd.DataFrame, base_dir: str | Path, partition_cols: list[str]
) -> str:
    """
    Save hive-style partitions: base_dir/col=value/...
    """
    base = ensure_dir(base_dir)
    if not partition_cols:
        return write_parquet(df, base)
    # naive partitioning
    keys = df[partition_cols].drop_duplicates()
    paths = []
    for _, row in keys.iterrows():
        subset = df.copy()
        mask = True
        part_path = base
        for c in partition_cols:
            v = row[c]
            mask = mask & (subset[c] == v)
            part_path = part_path / f"{c}={v}"
        ensure_dir(part_path)
        out = part_path / "part.parquet"
        subset[mask].to_parquet(out, index=False)
        paths.append(str(out))
    return str(base)
