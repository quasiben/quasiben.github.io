from __future__ import annotations

import gc
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import polars as pl
from cudf_polars.engine.default_singleton_engine import DefaultSingletonEngine
from cudf_polars.engine.options import StreamingOptions
from cudf_polars.engine.spmd import SPMDEngine

DATA_DIR = Path("/tmp/cudf-polars-timeseries-join-demo")
OUTPUT_PATH = Path("/tmp/cudf-polars-timeseries-join-demo/joined.parquet")
SPILL_DEVICE_LIMIT = "80%"
REQUIRED_TABLES = {
    "lineitem": "lineitem-*.parquet",
    "orders": "orders-*.parquet",
    "customer": "customer-*.parquet",
    "supplier": "supplier-*.parquet",
}


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


def format_bytes(nbytes: int | float) -> str:
    value = float(nbytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def stat_value(stats: dict[str, Any], name: str) -> float | int:
    entry = stats.get(name)
    if not isinstance(entry, dict):
        return 0
    return entry.get("value", 0)


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
) -> dict[str, Any]:
    """Run the TPC-H-ish multi-table join and write the joined rows to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.is_dir():
        shutil.rmtree(output_path)
    elif output_path.exists():
        output_path.unlink()

    build_query(data_dir).sink_parquet(output_path, engine=engine)
    return engine.global_statistics(clear=True).to_dict()


engine_init_start = time.perf_counter()
options = StreamingOptions(
    statistics=True,
    spill_device_limit=SPILL_DEVICE_LIMIT,
)
engine = SPMDEngine.from_options(options)
engine_init_seconds = time.perf_counter() - engine_init_start

query_start = time.perf_counter()
stats = run_query(engine=engine)
query_seconds = time.perf_counter() - query_start
print(stats)

d2h = stat_value(stats, "copy-device-to-host-bytes")
h2d = stat_value(stats, "copy-host-to-device-bytes")
d2h_time = stat_value(stats, "copy-device-to-host-time")
h2d_time = stat_value(stats, "copy-host-to-device-time")

print(f"Engine initialization took {engine_init_seconds:.2f} seconds")
print(f"Spill device limit: {SPILL_DEVICE_LIMIT}")
print(f"GPU query write took {query_seconds:.2f} seconds")
print(
    f"Total GPU path took "
    f"{engine_init_seconds + query_seconds:.2f} seconds"
)
print(f"Device-to-host copies: {format_bytes(d2h)}")
print(f"host-to-device copies: {format_bytes(h2d)}")
print(f"Device-to-host copy time: {d2h_time:.2f} seconds")
print(f"host-to-device copy time: {h2d_time:.2f} seconds")
print(f"Total spill copy time: {d2h_time + h2d_time:.2f} seconds")
