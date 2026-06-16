---
title: N + 1 Devices
date: 2026-06-13
author: Benjamin Zaitlen
slug: n-plus-one-devices
draft: true
---


**cuDF-Polars can scale a familiar Polars query across multiple GPUs with only a few configuration changes**

# N + 1 Devices

With cuDF-Polars 26.06, we have a new execution backend: RapidsMPF. RapidsMPF handles both execution and the distributed collectives needed for multi-GPU, out-of-core algorithms. That's a lot of nouns -- more simply, RapidsMPF gives cuDF-Polars a way to coordinate work and move data across workers/devices/ranks/etc.


In this post I want to start exploring the multi-GPU capabilities of cuDF-Polars. N+1 of anything is usually an advanced feature, or at least an intermediate one. It does not have to feel that way forever: NumPy gives users concurrency without making them think too much about the underlying hardware.   *That's probably the decades of labor in underlying libraries like LAPACK/BLAS but also thoughtful API design.*   The GPU analytics world is still a somewhat nascent adventure and we collectively are exploring how to deliver speed-of-light performance and ease of use without requiring extreme levels of expertise.  For these reasons, the multi-gpu experience is opt-in and the user should be more cognizant of what they are opting into, without having to deeply understand the underlying mechanics of GPU hardware or software.

With that said, if you find yourself (or require being) on a single-node multi-GPU (SNMG) machine you **CAN** leverage all this hardware with just a few additional engine configurations to otherwise standard Polars code:

Recall that the out-of-box experience for most cuDF-Polars users is as simple as possbile single parameter change: `collect(engine='gpu')`.  Now we are going to take that advanced step in defining the engine explicitly. 

```python
from cudf_polars.engine.ray import RayEngine

engine =  RayEngine()
result = (
    pl.scan_parquet("/data/*.parquet")
        .filter(pl.col("amount") > 100)
        .group_by("customer_id")
        .agg(pl.col("amount").sum())
        .collect(engine=engine)
    )
```

Hmmm, actually, not too bad.  It's quite simple in fact.  The above will automatically start using all the GPUs.  It even comes in [ContextManager](https://docs.python.org/3/library/contextlib.html) form for easy cleanup:

```python
from cudf_polars.engine.ray import RayEngine

with RayEngine() as engine:
    result = (
        pl.scan_parquet("/data/*.parquet")
          .filter(pl.col("amount") > 100)
          .group_by("customer_id")
          .agg(pl.col("amount").sum())
          .collect(engine=engine)
    )
```

This will automatically, start one ray worker per GPU and cuDF-Polars (RapidsMPF) will execute all the work evenly across each GPU.  There is no scheduler here for those familiar with task based systems like Spark.  Instead, all workers will get the same execution graph (a physical plan) and operate independently.  Synchronization points are inserted into the physical plan where communication is required: groupby-aggregations, join/merges, etc.  These higher level operations are composed of RapidsMPF atomic collectives common in distributed computing: shuffle (all-to-all), allgather, allreduce, sparse-all-to-all, etc.  For now, we'll stay above these details and continue exploring what kind of options are exposed to us.

I was able to get some time on a machine with 4 [T4s](https://www.nvidia.com/en-us/data-center/tesla-t4/). These are older and a bit smaller compared with more contemporary devices -- only 16GBs of VRAM/GPU but with four we have a whopping 64GBs!

```bash
$ nvidia-smi
Tue Jun 16 16:59:53 2026
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 580.159.04             Driver Version: 580.159.04     CUDA Version: 13.0     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  Tesla T4                       Off |   00000000:60:00.0 Off |                    0 |
| N/A   26C    P8             12W /   70W |       0MiB /  15360MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
|   1  Tesla T4                       Off |   00000000:61:00.0 Off |                    0 |
| N/A   35C    P8              9W /   70W |       0MiB /  15360MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
|   2  Tesla T4                       Off |   00000000:DA:00.0 Off |                    0 |
| N/A   40C    P8             13W /   70W |       0MiB /  15360MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
|   3  Tesla T4                       Off |   00000000:DB:00.0 Off |                    0 |
| N/A   32C    P8              9W /   70W |       0MiB /  15360MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+

+-----------------------------------------------------------------------------------------+
| Processes:                                                                              |
|  GPU   GI   CI              PID   Type   Process name                        GPU Memory |
|        ID   ID                                                               Usage      |
|=========================================================================================|
|  No running processes found                                                             |
+-----------------------------------------------------------------------------------------+
```

Make sure you install cudf-polars with Ray:

> python -m pip install cudf-polars-cu13[ray]


## Speed Demons of the High Seas

We are going to play with vessel traffic data collected and maintained by the U.S. Coast Guard: [Automatic Identification System (AIS) Vessel Data](https://hub.marinecadastre.gov/pages/vesseltraffic). It has a nice mix of traits for GPU analytics: geospatial data, time-series, scale, skew, and some oddities to make the results fun without requiring deep maritime knowledge.

> I downloaded all of 2025 and converted from CSV to Parquet with snappy compression.  The first quarter (Jan-Feb) ~19GBs on disk and uncompressed it's ~78GBs.  I'll limit myself to the first quarter.  We'll still need multiple GPUs, we'll need spilling, but workflows should also finish in a more reasonable amount of time

A more interesting analysis than simple scan-and-filter exploration is cohort analysis. Vessels are organized by group and type: Sailing, Fishing, Cargo, Passenger, Tug, and so on. The data also contains a velocity-like measurement, Speed over Ground (`SOG`). Let's find vessels whose speed is more than 10x their cohort average.


We could write this as an explicit `group_by`, join the cohort average back to the original table, and then filter:

```
cohort_avg = (
    df
    .group_by("VesselType", "Status")
    .agg(pl.col("SOG").mean().alias("cohort_avg_speed"))
)

return (
    df
    .join(cohort_avg, on=["VesselType", "Status"], how="left")
    .filter(pl.col("SOG") > (pl.col("cohort_avg_speed") * 10))
)
```

This is costly because it introduces two global synchronization points: `group_by` and `join`. Instead, we can remove the explicit join and use Polars’ [over](https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.over.html) expression to compute the cohort average in place.

```python
def build_speed_anomaly_query(path: str = DATA_GLOB) -> pl.LazyFrame:
    """Flag vessels travelling 10x faster than their cohort average (VesselType + Status).
    """
    return (
        pl.scan_parquet(path)
        .filter(pl.col("VesselType").is_not_null() & pl.col("Status").is_not_null())
        .with_columns(
            pl.col("SOG")
            .mean()
            .over(["VesselType", "Status"])
            .alias("cohort_avg_speed")
        )
        .with_columns(
            (pl.col("SOG") > (pl.col("cohort_avg_speed") * 10)).alias("is_speed_anomaly")
        )
        .filter(pl.col("is_speed_anomaly"))
    )
```

The defaults are intentionally simple: `RayEngine` uses all visible GPUs and starts one Ray worker per device. We can tune the engine later, but first let's run something real enough to stress the system.

```python
with RayEngine.from_options() as engine:
    query.sink_parquet(OUTPUT_DIR, engine=engine)
```

And this immediately fails with an OOM -- a not uncommon user experience :)

```bash
(RankActor pid=699246) [2026-06-16 17:07:10,190 E 699246 701128] logging.cc:118: Unhandled exception: N3rmm13out_of_memoryE. what(): std::bad_alloc: out_of_memory: CUDA error (failed to allocate 368974272 bytes) at
: /tmp/conda-bld-output/bld/rattler-build_librmm/work/cpp/src/mr/cuda_async_view_memory_resource.cpp:43: cudaErrorMemoryAllocation out of memory /localhome/local-bzaitlen/rapids-2608-nightly/lib/python3.14/site-pac
kages/ray/_raylet.so(+0x15d4368) [0x7bc9539d4368] ray::operator<<()
```

Often though, we need to tune the system and may want to configure various parameters in cuDF-Polars, RapidsMPF, or even Ray.  Still, the defaults should be a good starting point and defaults for RayEngine are to use all devices on the system.  It's recommended to use the [StreamingOptions](https://docs.rapids.ai/api/cudf/stable/cudf_polars/options/#configuration-options) dataclass.  We can set everything related to the how the query is executed and tune...: `spill_device_limit`, `fallback_mode`, `target_partition_size` , etc.  We can also configure the Ray Actors:


The default spill_device_limit is 80%.  Let's lower and learn how to configure the multi-gpu RayEngine. I'm also going to add some ray configurations so I can view [the dashboard](https://docs.ray.io/en/latest/ray-observability/getting-started.html) remotely:

```python
opts = StreamingOptions(
    spill_device_limit="70%",
)

ray_init_options = {
    "include_dashboard": True,
    "dashboard_host": "0.0.0.0",
    "dashboard_port": 8265,
}


with RayEngine.from_options(opts, ray_init_options=ray_init_options) as engine:
    query.sink_parquet(OUTPUT_DIR, engine=engine)
```

This time the query runs cleanly. In the GIF below, all four T4s turn on and stay busy for most of the query. That is the main thing I wanted to see. Our cycle has been:
1. Take a Polars query and naively run it on multiple GPUs.
2. Hit an OOM.
3. Tune the engine and re-run.
4. Success!

![Speed anomaly query](n-plus-one-devices/speed_anomaly_query.gif)

I'm not terribly confident in the domain analysis. On the surface it seems plausible, but it probably deserves more scrutiny from someone who knows AIS data better than I do. As a systems exercise, though, this is exactly the kind of workload I hoped for: large enough to require multi-GPU execution, memory intensive enough to require spilling, and communication-heavy enough to make the execution engine do real distributed work.

```
In [10]: (
    ...:       pl.scan_parquet('/localhome/local-bzaitlen/ais-cudf-polars/output/result/*.parquet')
    ...:       .group_by('VesselType')
    ...:       .agg(pl.len().alias('anomaly_count'))
    ...:       .sort('anomaly_count', descending=True)
    ...:       .limit(5)
    ...:       .collect()
    ...:       .with_columns(
    ...:           pl.col('VesselType').map_elements(lambda x: VESSEL_TYPES.get(x, f"Unknown ({x})"), retur
       ⋮ n_dtype=pl.String).alias('VesselTypeName')
    ...:       )
    ...:   )
Out[10]:
shape: (5, 3)
┌────────────┬───────────────┬───────────────────┐
│ VesselType ┆ anomaly_count ┆ VesselTypeName    │
│ ---        ┆ ---           ┆ ---               │
│ i64        ┆ u32           ┆ str               │
╞════════════╪═══════════════╪═══════════════════╡
│ 52         ┆ 547390        ┆ Tug               │
│ 30         ┆ 262985        ┆ Fishing           │
│ 70         ┆ 252750        ┆ Cargo             │
│ 60         ┆ 216759        ┆ Passenger         │
│ 51         ┆ 193884        ┆ Search and rescue │
└────────────┴───────────────┴───────────────────┘
```

Turns out it's mostly the Tugs which operate at 10x the average speed.  I suppose that makes some sense; [little toot](https://en.wikipedia.org/wiki/Little_Toot) likes the igure-eights in the harbor but can only go so fast when pulling in the big ocean liners