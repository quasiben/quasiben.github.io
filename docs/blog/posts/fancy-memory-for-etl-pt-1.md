---
title: Fancy Memory for ETL pt. 1
date: 2026-06-04
author: Benjamin Zaitlen
slug: fancy-memory-for-etl
---

# Fancy Memory for ETL pt. 1

cuDF-Polars 26.06 was recently released, and there are several intermediate and advanced features I want to highlight over the next few weeks and months. Let’s start with a longstanding challenge for GPU  analytics: running beyond available GPU memory.  In the following example, we generate data larger than the GPU memory available and force the engine into a spilling path. Spilling means moving data from device memory to host memory when the operations would otherwise exceed available VRAM and cause an OOM.

> TLDR: Spilling makes larger-than-VRAM GPU analytics possible, but memory movement is not free. For longer-running workflows or repeated queries using pinned host memory can materially reduce transfer overhead and decrease overall execution time. 

For this example I found some time on an [L40 GPU](https://www.nvidia.com/en-us/data-center/l40/) which you can easily rent on [aws](https://instances.vantage.sh/?id=34b8d04c6b05a2a46cc2548776a6d96b63172156) or any other major CSP.  The machine also comes with a considerable amount of CPU resources: 128 GB of RAM and an AMD EPYC 7313P 16-Core Processor.

Let's first start by generating some data.  I had an agent build me some wholesale order like data with tables: lineitem, orders, customers, and suppliers.

*[Data Generation Code](/static/code-snippets/cudf-polars-memory/generate-data.py)*

```
Rows: lineitem=95,000,000, orders=23,750,000, customer=2,375,000, supplier=200,000
Generated 3.74 GiB compressed on disk
Data generation took 39.80 seconds
```

*As an aside, using AI agents to quickly generate data is handy. For synthetic examples like this where I'm more interested in showcasing a feature than a specific use case, it’s just a faster way to get to the actual code and start benchmarking.*

Great! We've got some data, how about a query?  Let's go after a more complex query, not too long -- something with joins and some zest -- we can to stress memory:

*[Full working example](/static/code-snippets/cudf-polars-memory/cudf-join-gpu-pinned-spill.py)*


```python
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
```

Uncompressed the data is ~50GB, so we'd expect some spilling.  With cudf-polars 26.06, we have a new backend to execute and manage memory: [RapidsMPF](https://github.com/rapidsai/rapidsmpf).  Let's see what happens when we naively run the query and write the results back to disk:

```python
build_query(data_dir).sink_parquet(output_path, engine='gpu')
```

On this L40, the query runs in ~27s.  You may or may not know this, but cuDF-polars is doing something on your behalf.  It's spilling and seems like it has a reasonable default.  By default,  at 80% of the device memory,  cuDF-Polars will start spilling from device-to-host.   Let's turn off spilling and see what happens.  For this we are going to explicitly configure the GPU engine and options for cuDF-polars:

```python

options = StreamingOptions(
    spill_device_limit="100%"
)
engine = SPMDEngine.from_options(options)

build_query(data_dir).sink_parquet(output_path, engine=engine)
``` 

Running this we get an error!

> MemoryError: Try lowering `target_partition_size` (current 1207644979) and/or RAPIDSMPF_SPILL_DEVICE_LIMIT (default '80%') to reduce peak memory.
See https://docs.rapids.ai/api/cudf/stable/cudf_polars/memory_errors/ for troubleshooting guidance.
Original error:
std::bad_alloc: out_of_memory: CUDA error (failed to allocate 576000000 bytes) at: /tmp/conda-bld-output/bld/rattler-build_librmm/work/cpp/include/rmm/mr/cuda_async_view_memory_resource.hpp:87: cudaErrorMemoryAlloc

The default spill device limit of 80% feels like a good balance between performance and usability. It gives users a working out-of-core GPU analytics experience without requiring them to understand memory tuning up front. Now let’s keep the default spill limit (80%), enable statistics, and inspect what actually happened during execution.

```python
options = StreamingOptions(
    statistics=True,
    fallback_mode="raise",
    spill_device_limit=SPILL_DEVICE_LIMIT,
)
engine = SPMDEngine.from_options(options)

build_query(data_dir).sink_parquet(output_path, engine=engine)
```

After running, calling `engine.global_statistics(clear=True).to_dict()` gives us a dictionary of stats collected by cudf-polars and RapidsMPF.  The spill-related counters are the interesting ones for us in this experiment:

| Mode | Direction | Bytes counter | Count | Total bytes | Max transfer | Time |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Pageable host spilling | Device -> host | `copy-device-to-host-bytes` | 24 | 70.34 GB | 3.58 GB | 8.06 s |
| Pageable host spilling | Host -> device | `copy-host-to-device-bytes` | 24 | 70.34 GB | 3.58 GB | 4.06 s |

Of the ~27s of execution, the workflow spends ~12s just copying spilled data between host and device.  It moves about 70GB in each direction, so roughly 140GB crosses the PCIe boundary.  That movement is expensive because of transfer bandwidth, allocating pageable memory on the host, and device synchronization points that can briefly pause streaming execution.  If we use pinned memory and [optimize data transfer between device and host](https://developer.nvidia.com/blog/how-optimize-data-transfers-cuda-cc/) we can speed up our execution BUT it comes with an initialization charge.  It does take time for the OS to pre-allocate this memory upfront:

```python
options = StreamingOptions(
    statistics=True,
    fallback_mode="raise",
    pinned_memory=True,
    pinned_initial_pool_size=100 * 1024**3,
    spill_device_limit="80%",
)
```

Engine initialization took ~16 seconds but now total execution time is ~23s.  As execution time goes up, this one time initialization cost matters less and less.  The spill volume is about the same, but the transfer portion is much faster in both directions

| Mode | Direction | Bytes counter | Count | Total bytes | Max transfer | Time |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Pinned host spilling | Device -> pinned host | `copy-device-to-pinned_host-bytes` | 23 | 67.47 GB | 3.58 GB | 2.64 s |
| Pinned host spilling | Pinned host -> device | `copy-pinned_host-to-device-bytes` | 23 | 67.47 GB | 3.58 GB | 3.15 s |

| Mode | Bytes copied each direction | Total copy time | Query time | Notes |
| --- | ---: | ---: | ---: | --- |
| Pageable host spilling | 70.34 GB | 12.11 s |  | Regular host spilling copies through pageable host memory. |
| Pinned host spilling | 67.47 GB | 5.79 s |  | Pinned transfers reduced copy time by 52.2%. |

Spilling is a necessary and critical component to building accelerated engines for both CPU and for GPU where memory is further constrained.  Pinned memory changes the cost of that movement.  I expect this feature to be more widely but users should be cognizant of the one time initialization and an opt-in policy helps maintain expectations of why startup time may be slower but how to achieve greater performance with simple configuration changes.

The stats tell us pinned memory cuts transfer time substantially, but they do not show how that work overlaps with the rest of the query. In part 2, we’ll open Nsight Systems traces and look at what the pipeline is actually doing.
