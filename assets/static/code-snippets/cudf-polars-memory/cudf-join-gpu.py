from __future__ import annotations

import gc
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import polars as pl

DATA_DIR = Path("/tmp/cudf-polars-timeseries-join-demo")
OUTPUT_PATH = Path("/tmp/cudf-polars-timeseries-join-demo/joined.parquet")
REQUIRED_TABLES = {
    "lineitem": "lineitem-*.parquet",
    "orders": "orders-*.parquet",
    "customer": "customer-*.parquet",
    "supplier": "supplier-*.parquet",
}


def validate_input_files(data_dir: Path = DATA_DIR) -> None:
    missing = [
        f"{name} ({pattern})"
        for name, pattern in REQUIRED_TABLES.items()
        if not any(data_dir.glob(pattern))
    ]
    if missing:
        missing_text = ", ".join(missing)
        raise SystemExit(
            f"Missing input parquet files in {data_dir}: {missing_text}.\n"
            "Run `python generate-data.py` to create the full dataset."
        )


def clear_os_cache(label: str) -> None:
    """Drop Linux filesystem caches before timing a benchmark run."""
    print(f"Clearing OS cache before {label}...", flush=True)
    gc.collect()
    subprocess.run(["sync"], check=True)

    if os.geteuid() == 0:
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    else:
        subprocess.run(
            ["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
            check=True,
        )

    print("OS cache cleared.", flush=True)


def build_query(data_dir: Path = DATA_DIR) -> pl.LazyFrame:
    lineitem = pl.scan_parquet(str(data_dir / "lineitem-*.parquet"))
    orders = pl.scan_parquet(str(data_dir / "orders-*.parquet"))
    customer = pl.scan_parquet(str(data_dir / "customer-*.parquet"))
    supplier = pl.scan_parquet(str(data_dir / "supplier-*.parquet"))

    return (
        lineitem.join(orders, on="orderkey", how="inner")
        .join(customer, on="custkey", how="inner")
        .join(supplier, on="suppkey", how="inner")
        .with_columns(
            [
                (pl.col("extendedprice") * (1.0 - pl.col("discount"))).alias(
                    "net_revenue"
                ),
                (pl.col("quantity") * pl.col("supply_cost")).alias("supply_value"),
                (pl.col("ship_day") - pl.col("order_day")).alias("ship_lag_days"),
            ]
        )
    )


def run_query(
    engine: Any,
    data_dir: Path = DATA_DIR,
    output_path: Path = OUTPUT_PATH,
) -> None:
    """Run the TPC-H-ish multi-table join and write the joined rows to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.is_dir():
        shutil.rmtree(output_path)
    elif output_path.exists():
        output_path.unlink()

    build_query(data_dir).sink_parquet(output_path, engine=engine)


validate_input_files()
clear_os_cache("GPU query")
query_start = time.perf_counter()
run_query(engine="gpu")
query_seconds = time.perf_counter() - query_start
print(f"GPU query write took {query_seconds:.2f} seconds")
