from __future__ import annotations

import gc
import math
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

DATA_DIR = Path("/tmp/cudf-polars-timeseries-join-demo")

PARQUET_COMPRESSION = "zstd"

# Target the estimated in-GPU size of the joined rows, not compressed bytes on disk.
JOIN_OUTPUT_TARGET_GPU_BYTES = 50 * 1024**3

LINEITEM_FILE_ROWS = 1_000_000
ORDERS_FILE_ROWS = 500_000
CUSTOMER_FILE_ROWS = 250_000
SUPPLIER_FILE_ROWS = 50_000

AVG_LINEITEMS_PER_ORDER = 4
CUSTOMERS_PER_ORDER = 10
SUPPLIER_ROWS = 200_000
PART_ROWS = 20_000_000

LINE_COMMENT_WIDTH = 96
ORDER_COMMENT_WIDTH = 64
CUSTOMER_COMMENT_WIDTH = 96
SUPPLIER_COMMENT_WIDTH = 64

STRING_OFFSET_BYTES = 4
OUTPUT_STRING_COLUMNS = 12
OUTPUT_STRING_BYTES_PER_ROW = (
    LINE_COMMENT_WIDTH
    + ORDER_COMMENT_WIDTH
    + CUSTOMER_COMMENT_WIDTH
    + SUPPLIER_COMMENT_WIDTH
    + len("N")
    + len("O")
    + len("TRUCK")
    + len("1-URGENT")
    + len("Clerk#000000001")
    + len("BUILDING")
    + len("Customer#000000001")
    + len("Supplier#000001")
)
OUTPUT_FIXED_BYTES_PER_ROW = (
    5 * 8  # orderkey, custkey, partkey, suppkey, extendedprice
    + 5 * 8  # quantity, discount, tax, totalprice, acctbal
    + 3 * 8  # supply_cost, net_revenue, supply_value
    + 7 * 4  # linenumber, ship_day, order_day, shippriority, nation keys, region
)
JOIN_OUTPUT_GPU_BYTES_PER_ROW = (
    OUTPUT_FIXED_BYTES_PER_ROW
    + OUTPUT_STRING_BYTES_PER_ROW
    + OUTPUT_STRING_COLUMNS * STRING_OFFSET_BYTES
)


def rows_for_target_bytes(target_bytes: int, bytes_per_row: int, file_rows: int) -> int:
    rows = math.ceil(target_bytes / bytes_per_row)
    return math.ceil(rows / file_rows) * file_rows


LINEITEM_ROWS = rows_for_target_bytes(
    JOIN_OUTPUT_TARGET_GPU_BYTES,
    JOIN_OUTPUT_GPU_BYTES_PER_ROW,
    LINEITEM_FILE_ROWS,
)
ORDER_ROWS = math.ceil(LINEITEM_ROWS / AVG_LINEITEMS_PER_ORDER)
CUSTOMER_ROWS = max(1_000_000, math.ceil(ORDER_ROWS / CUSTOMERS_PER_ORDER))


def gib(value: int | float) -> float:
    return value / 1024**3


def fixed_width_u12(value: pl.Expr) -> pl.Expr:
    padded = (value % 1_000_000_000_000) + 1_000_000_000_000
    return padded.cast(pl.String).str.slice(1, 12)


def string_payload(row_ids: pl.Expr, prefix: str, width: int) -> pl.Expr:
    if width <= len(prefix):
        return pl.lit(prefix[:width])

    filler_parts = math.ceil((width - len(prefix)) / 12)
    parts = [pl.lit(prefix)]
    for salt in range(filler_parts):
        mixed = (
            (row_ids + salt * 104_729) * (1_000_003 + salt * 2)
            + (salt + 1) * 9_176_291
        )
        parts.append(fixed_width_u12(mixed))

    return pl.concat_str(parts).str.slice(0, width)


def categorical(index: pl.Expr, values: list[str]) -> pl.Expr:
    expr = pl.lit(values[-1])
    size = len(values)
    for position, value in reversed(list(enumerate(values[:-1]))):
        expr = pl.when((index % size) == position).then(pl.lit(value)).otherwise(expr)
    return expr


def chunk_ranges(rows: int, chunk_rows: int) -> list[tuple[int, int]]:
    return [(start, min(start + chunk_rows, rows)) for start in range(0, rows, chunk_rows)]


def make_lineitem(start: int, stop: int, payload_width: int) -> Any:
    row_id = pl.Series("row_id", range(start, stop), dtype=pl.Int64)

    return pl.DataFrame({"row_id": row_id}).select(
        [
            (pl.col("row_id") // AVG_LINEITEMS_PER_ORDER + 1).alias("orderkey"),
            ((pl.col("row_id") * 13) % PART_ROWS + 1).cast(pl.Int64).alias("partkey"),
            ((pl.col("row_id") * 17) % SUPPLIER_ROWS + 1).cast(pl.Int64).alias(
                "suppkey"
            ),
            (pl.col("row_id") % AVG_LINEITEMS_PER_ORDER + 1).cast(pl.Int32).alias(
                "linenumber"
            ),
            (pl.col("row_id") % 50 + 1).cast(pl.Float64).alias("quantity"),
            (((pl.col("row_id") * 97) % 200_000).cast(pl.Float64) / 100 + 10).alias(
                "extendedprice"
            ),
            ((pl.col("row_id") % 11).cast(pl.Float64) / 100).alias("discount"),
            ((pl.col("row_id") % 9).cast(pl.Float64) / 100).alias("tax"),
            categorical(pl.col("row_id") // 7, ["N", "R", "A"]).alias("returnflag"),
            categorical(pl.col("row_id") // 5, ["O", "F"]).alias("linestatus"),
            categorical(pl.col("row_id") // 11, ["AIR", "TRUCK", "SHIP", "RAIL", "MAIL"]).alias(
                "shipmode"
            ),
            ((pl.col("row_id") * 17) % 2_557).cast(pl.Int32).alias("ship_day"),
            string_payload(pl.col("row_id"), "line-", payload_width).alias(
                "line_comment"
            ),
        ]
    )


def make_orders(start: int, stop: int, payload_width: int) -> Any:
    row_id = pl.Series("row_id", range(start, stop), dtype=pl.Int64)

    return pl.DataFrame({"row_id": row_id}).select(
        [
            (pl.col("row_id") + 1).alias("orderkey"),
            ((pl.col("row_id") * 7) % CUSTOMER_ROWS + 1).cast(pl.Int64).alias("custkey"),
            categorical(pl.col("row_id"), ["O", "F", "P"]).alias("orderstatus"),
            (((pl.col("row_id") * 1201) % 10_000_000).cast(pl.Float64) / 100 + 100).alias(
                "totalprice"
            ),
            ((pl.col("row_id") * 13) % 2_557).cast(pl.Int32).alias("order_day"),
            categorical(
                pl.col("row_id") // 3,
                ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"],
            ).alias("orderpriority"),
            pl.concat_str(
                [pl.lit("Clerk#"), fixed_width_u12(pl.col("row_id")).str.slice(3, 9)]
            ).alias("clerk"),
            (pl.col("row_id") % 5).cast(pl.Int32).alias("shippriority"),
            string_payload(pl.col("row_id"), "order-", payload_width).alias(
                "order_comment"
            ),
        ]
    )


def make_customers(start: int, stop: int, payload_width: int) -> Any:
    row_id = pl.Series("row_id", range(start, stop), dtype=pl.Int64)

    return pl.DataFrame({"row_id": row_id}).select(
        [
            (pl.col("row_id") + 1).alias("custkey"),
            pl.concat_str(
                [pl.lit("Customer#"), fixed_width_u12(pl.col("row_id")).str.slice(3, 9)]
            ).alias("customer_name"),
            (pl.col("row_id") % 25).cast(pl.Int32).alias("nationkey"),
            (pl.col("row_id") % 5).cast(pl.Int32).alias("regionkey"),
            categorical(
                pl.col("row_id"),
                ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"],
            ).alias("marketsegment"),
            (((pl.col("row_id") * 31) % 1_000_000).cast(pl.Float64) / 100 - 999.99).alias(
                "acctbal"
            ),
            pl.concat_str([
                fixed_width_u12(pl.col("row_id")).str.slice(0, 3),
                pl.lit("-"),
                fixed_width_u12(pl.col("row_id") * 17).str.slice(0, 3),
                pl.lit("-"),
                fixed_width_u12(pl.col("row_id") * 97).str.slice(0, 4),
            ]).alias("phone"),
            string_payload(pl.col("row_id"), "customer-", payload_width).alias(
                "customer_comment"
            ),
        ]
    )


def make_suppliers(start: int, stop: int, payload_width: int) -> Any:
    row_id = pl.Series("row_id", range(start, stop), dtype=pl.Int64)

    return pl.DataFrame({"row_id": row_id}).select(
        [
            (pl.col("row_id") + 1).alias("suppkey"),
            pl.concat_str(
                [pl.lit("Supplier#"), fixed_width_u12(pl.col("row_id")).str.slice(6, 6)]
            ).alias("supplier_name"),
            (pl.col("row_id") % 25).cast(pl.Int32).alias("supplier_nationkey"),
            (((pl.col("row_id") * 41) % 500_000).cast(pl.Float64) / 100 + 1).alias(
                "supply_cost"
            ),
            string_payload(pl.col("row_id"), "supplier-", payload_width).alias(
                "supplier_comment"
            ),
        ]
    )


def write_parquet(frame: Any, path: Path) -> int:
    frame.write_parquet(path, compression=PARQUET_COMPRESSION)
    return path.stat().st_size


def write_table(
    data_dir: Path,
    name: str,
    file_prefix: str,
    make_table: Any,
    rows: int,
    file_rows: int,
    payload_width: int,
) -> int:
    print(
        f"Writing {name}: {rows:,} rows in chunks of {file_rows:,}, "
        f"payload width {payload_width:,}",
        flush=True,
    )

    total_bytes = 0
    for idx, (start, stop) in enumerate(chunk_ranges(rows, file_rows)):
        frame = make_table(start, stop, payload_width)
        try:
            total_bytes += write_parquet(frame, data_dir / f"{file_prefix}-{idx:02d}.parquet")
        finally:
            del frame
            gc.collect()

    print(
        f"Wrote {name}: {gib(total_bytes):.2f} GiB compressed "
        f"({total_bytes:,} bytes)",
        flush=True,
    )
    return total_bytes


def estimate_join_output_gpu_bytes() -> int:
    return LINEITEM_ROWS * JOIN_OUTPUT_GPU_BYTES_PER_ROW


def generate_data(data_dir: Path = DATA_DIR) -> dict[str, Any]:
    """Generate TPC-H-ish tables sized for an estimated cuDF joined footprint."""
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)

    join_gpu_bytes = estimate_join_output_gpu_bytes()
    print(
        f"Estimated joined output GPU footprint: {gib(join_gpu_bytes):.2f} GiB "
        f"for target {gib(JOIN_OUTPUT_TARGET_GPU_BYTES):.2f} GiB",
        flush=True,
    )
    print(
        f"Rows: lineitem={LINEITEM_ROWS:,}, orders={ORDER_ROWS:,}, "
        f"customer={CUSTOMER_ROWS:,}, supplier={SUPPLIER_ROWS:,}",
        flush=True,
    )

    lineitem_bytes = write_table(
        data_dir,
        "lineitem",
        "lineitem",
        make_lineitem,
        LINEITEM_ROWS,
        LINEITEM_FILE_ROWS,
        LINE_COMMENT_WIDTH,
    )
    orders_bytes = write_table(
        data_dir,
        "orders",
        "orders",
        make_orders,
        ORDER_ROWS,
        ORDERS_FILE_ROWS,
        ORDER_COMMENT_WIDTH,
    )
    customer_bytes = write_table(
        data_dir,
        "customer",
        "customer",
        make_customers,
        CUSTOMER_ROWS,
        CUSTOMER_FILE_ROWS,
        CUSTOMER_COMMENT_WIDTH,
    )
    supplier_bytes = write_table(
        data_dir,
        "supplier",
        "supplier",
        make_suppliers,
        SUPPLIER_ROWS,
        SUPPLIER_FILE_ROWS,
        SUPPLIER_COMMENT_WIDTH,
    )

    return {
        "data_dir": str(data_dir),
        "lineitem_rows": LINEITEM_ROWS,
        "lineitem_bytes": lineitem_bytes,
        "orders_rows": ORDER_ROWS,
        "orders_bytes": orders_bytes,
        "customer_rows": CUSTOMER_ROWS,
        "customer_bytes": customer_bytes,
        "supplier_rows": SUPPLIER_ROWS,
        "supplier_bytes": supplier_bytes,
        "join_gpu_bytes": join_gpu_bytes,
        "total_bytes": lineitem_bytes + orders_bytes + customer_bytes + supplier_bytes,
    }


if __name__ == "__main__":
    generate_start = time.perf_counter()
    data = generate_data()
    generate_seconds = time.perf_counter() - generate_start

    print(
        f"Generated {data['lineitem_rows']:,} lineitem rows "
        f"({gib(data['lineitem_bytes']):.2f} GiB compressed) in {data['data_dir']}"
    )
    print(
        f"Generated {data['orders_rows']:,} orders rows "
        f"({gib(data['orders_bytes']):.2f} GiB compressed)"
    )
    print(
        f"Generated {data['customer_rows']:,} customer rows "
        f"({gib(data['customer_bytes']):.2f} GiB compressed)"
    )
    print(
        f"Generated {data['supplier_rows']:,} supplier rows "
        f"({gib(data['supplier_bytes']):.2f} GiB compressed)"
    )
    print(f"Estimated joined output GPU footprint: {gib(data['join_gpu_bytes']):.2f} GiB")
    print(f"Generated {gib(data['total_bytes']):.2f} GiB compressed on disk")
    print(f"Data generation took {generate_seconds:.2f} seconds")
