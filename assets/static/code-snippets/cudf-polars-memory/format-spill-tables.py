from __future__ import annotations

from typing import Any


# Paste your raw dictionaries here.
PINNED_STATS: dict[str, dict[str, Any]] = {'alloc-device-bytes': {'count': 23, 'value': 67474800000.0, 'max': 3578400000.0}, 'alloc-device-stream-delay': {'count': 23, 'value': 0.0278158, 'max': 0.0186334}, 'alloc-device-time': {'count': 23, 'value': 0.0218596, 'max': 0.00621748}, 'alloc-host-bytes': {'count': 1, 'value': 152, 'max': 152}, 'alloc-host-stream-delay': {'count': 1, 'value': 0.000316143, 'max': 0.000316143}, 'alloc-host-time': {'count': 1, 'value': 1.0252e-05, 'max': 1.0252e-05}, 'alloc-pinned_host-bytes': {'count': 23, 'value': 67474800000.0, 'max': 3578400000.0}, 'alloc-pinned_host-stream-delay': {'count': 23, 'value': 0.0260031, 'max': 0.021667}, 'alloc-pinned_host-time': {'count': 23, 'value': 0.0247736, 'max': 0.0244293}, 'copy-device-to-pinned_host-bytes': {'count': 23, 'value': 67474800000.0, 'max': 3578400000.0}, 'copy-device-to-pinned_host-stream-delay': {'count': 23, 'value': 0.0500188, 'max': 0.0244648}, 'copy-device-to-pinned_host-time': {'count': 23, 'value': 2.63955, 'max': 0.155761}, 'copy-pinned_host-to-device-bytes': {'count': 23, 'value': 67474800000.0, 'max': 3578400000.0}, 'copy-pinned_host-to-device-stream-delay': {'count': 23, 'value': 0.0212748, 'max': 0.0186164}, 'copy-pinned_host-to-device-time': {'count': 23, 'value': 3.14984, 'max': 0.245317}}


PAGEABLE_STATS: dict[str, dict[str, Any]] =  {'alloc-device-bytes': {'count': 24, 'value': 70338000000.0, 'max': 3578400000.0}, 'alloc-device-stream-delay': {'count': 24, 'value': 0.0131495, 'max': 0.00961614}, 'alloc-device-time': {'count': 24, 'value': 0.0244145, 'max': 0.00966167}, 'alloc-host-bytes': {'count': 25, 'value': 70338000000.0, 'max': 3578400000.0}, 'alloc-host-stream-delay': {'count': 25, 'value': 0.00389838, 'max': 0.000607729}, 'alloc-host-time': {'count': 25, 'value': 0.000751495, 'max': 0.000114679}, 'copy-device-to-host-bytes': {'count': 24, 'value': 70338000000.0, 'max': 3578400000.0}, 'copy-device-to-host-stream-delay': {'count': 24, 'value': 0.00224543, 'max': 0.000517845}, 'copy-device-to-host-time': {'count': 24, 'value': 8.05691, 'max': 0.500215}, 'copy-host-to-device-bytes': {'count': 24, 'value': 70338000000.0, 'max': 3578400000.0}, 'copy-host-to-device-stream-delay': {'count': 24, 'value': 0.00319362, 'max': 0.00184512}, 'copy-host-to-device-time': {'count': 24, 'value': 4.05551, 'max': 0.336025}}

# Fill these in from the run output.
PAGEABLE_QUERY_SECONDS: float | None = None
PINNED_QUERY_SECONDS: float | None = None
PINNED_ENGINE_INIT_SECONDS: float | None = None
PINNED_POOL_SIZE: str | None = None


def stat(stats: dict[str, dict[str, Any]], name: str, field: str = "value") -> float:
    entry = stats.get(name)
    if not isinstance(entry, dict):
        return 0.0
    value = entry.get(field, 0.0)
    if value is None:
        return 0.0
    return float(value)


def stat_count(stats: dict[str, dict[str, Any]], name: str) -> int:
    return int(stat(stats, name, "count"))


def format_gb(nbytes: float) -> str:
    return f"{nbytes / 1_000_000_000:.2f} GB"


def format_seconds(seconds: float | None) -> str:
    if seconds is None:
        return ""
    return f"{seconds:.2f} s"


def markdown_table(headers: list[str], rows: list[list[str]], aligns: list[str]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(aligns) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def spill_rows(
    *,
    mode: str,
    stats: dict[str, dict[str, Any]],
    d2h_label: str,
    h2d_label: str,
    d2h_bytes_key: str,
    h2d_bytes_key: str,
    d2h_time_key: str,
    h2d_time_key: str,
) -> list[list[str]]:
    return [
        [
            mode,
            d2h_label,
            f"`{d2h_bytes_key}`",
            str(stat_count(stats, d2h_bytes_key)),
            format_gb(stat(stats, d2h_bytes_key)),
            format_gb(stat(stats, d2h_bytes_key, "max")),
            format_seconds(stat(stats, d2h_time_key)),
        ],
        [
            mode,
            h2d_label,
            f"`{h2d_bytes_key}`",
            str(stat_count(stats, h2d_bytes_key)),
            format_gb(stat(stats, h2d_bytes_key)),
            format_gb(stat(stats, h2d_bytes_key, "max")),
            format_seconds(stat(stats, h2d_time_key)),
        ],
    ]


def copy_time(stats: dict[str, dict[str, Any]], d2h_time_key: str, h2d_time_key: str) -> float:
    return stat(stats, d2h_time_key) + stat(stats, h2d_time_key)


def summary_note(
    *,
    mode: str,
    copy_seconds: float,
    query_seconds: float | None,
    pageable_copy_seconds: float | None = None,
) -> str:
    if mode == "Pageable host spilling":
        if query_seconds:
            share = copy_seconds / query_seconds * 100
            return f"Regular host spilling spent about {share:.1f}% of query time in spill copies."
        return "Regular host spilling copies through pageable host memory."

    parts = []
    if pageable_copy_seconds:
        reduction = (1 - copy_seconds / pageable_copy_seconds) * 100
        parts.append(f"Pinned transfers reduced copy time by {reduction:.1f}%")
    else:
        parts.append("Pinned transfers use pinned host memory")
    if PINNED_ENGINE_INIT_SECONDS is not None:
        parts.append(f"but engine initialization added {PINNED_ENGINE_INIT_SECONDS:.2f} s up front")
    if PINNED_POOL_SIZE:
        parts.append(f"with a {PINNED_POOL_SIZE} pinned pool")
    return ", ".join(parts) + "."


def main() -> None:
    table_headers = [
        "Mode",
        "Direction",
        "Bytes counter",
        "Count",
        "Total bytes",
        "Max transfer",
        "Time",
    ]
    table_aligns = ["---", "---", "---", "---:", "---:", "---:", "---:"]

    pageable_rows = spill_rows(
        mode="Pageable host spilling",
        stats=PAGEABLE_STATS,
        d2h_label="Device -> host",
        h2d_label="Host -> device",
        d2h_bytes_key="copy-device-to-host-bytes",
        h2d_bytes_key="copy-host-to-device-bytes",
        d2h_time_key="copy-device-to-host-time",
        h2d_time_key="copy-host-to-device-time",
    )
    pinned_rows = spill_rows(
        mode="Pinned host spilling",
        stats=PINNED_STATS,
        d2h_label="Device -> pinned host",
        h2d_label="Pinned host -> device",
        d2h_bytes_key="copy-device-to-pinned_host-bytes",
        h2d_bytes_key="copy-pinned_host-to-device-bytes",
        d2h_time_key="copy-device-to-pinned_host-time",
        h2d_time_key="copy-pinned_host-to-device-time",
    )

    pageable_copy_seconds = copy_time(
        PAGEABLE_STATS, "copy-device-to-host-time", "copy-host-to-device-time"
    )
    pinned_copy_seconds = copy_time(
        PINNED_STATS,
        "copy-device-to-pinned_host-time",
        "copy-pinned_host-to-device-time",
    )
    summary_rows = [
        [
            "Pageable host spilling",
            format_gb(stat(PAGEABLE_STATS, "copy-device-to-host-bytes")),
            format_seconds(pageable_copy_seconds),
            format_seconds(PAGEABLE_QUERY_SECONDS),
            summary_note(
                mode="Pageable host spilling",
                copy_seconds=pageable_copy_seconds,
                query_seconds=PAGEABLE_QUERY_SECONDS,
            ),
        ],
        [
            "Pinned host spilling",
            format_gb(stat(PINNED_STATS, "copy-device-to-pinned_host-bytes")),
            format_seconds(pinned_copy_seconds),
            format_seconds(PINNED_QUERY_SECONDS),
            summary_note(
                mode="Pinned host spilling",
                copy_seconds=pinned_copy_seconds,
                query_seconds=PINNED_QUERY_SECONDS,
                pageable_copy_seconds=pageable_copy_seconds,
            ),
        ],
    ]

    print(markdown_table(table_headers, pageable_rows, table_aligns))
    print()
    print(markdown_table(table_headers, pinned_rows, table_aligns))
    print()
    print(
        markdown_table(
            [
                "Mode",
                "Bytes copied each direction",
                "Total copy time",
                "Query time",
                "Notes",
            ],
            summary_rows,
            ["---", "---:", "---:", "---:", "---"],
        )
    )


if __name__ == "__main__":
    main()
