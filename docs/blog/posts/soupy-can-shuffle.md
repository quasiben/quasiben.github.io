---
title: Yet Another Blog About Shuffling
date: 2026-09-01
author: Benjamin Zaitlen
slug: yabas
draft: true
---


**...**

Oof, yet another blog about shuffling: YABAS (I hope [Stephen Johnson](https://en.wikipedia.org/wiki/Stephen_C._Johnson) isn't too upset). Well, this will be a fun version at least. `Shuffling` can be used in a lot of contexts, let's start with one from yesteryear, Soupy Sales doing his famous Shuffle:

<!-- more -->

<p><center><img src="soupy-sales-shuffle.gif" alt="Soupy Sales shuffle"></center></p>

FUN!

Ok, shuffling data is the crux of structured data analytics distributed or otherwise. It's a core component of key data operations like: join, groupby, merge, sort, etc. A full shuffle of all the data is costly and many sophisticated techniques have been developed to *avoid* this operation as much as possible. Shuffles aren't particularly compute intesive, yes you need to need calculate some hashes but this operation is fairly cheap. They are expensive instead for other reasons:

1. Memory intesive: shuffles can require holding onto a full copy of all the data or, in streaming cases, memory pressure can build and cause OOMs
1. Transport: The data physically has to be moved from Process A->Process B or Node A->Node B so it can only move at speed of the transport layer
1. Serialization: accelerated network transport is magical and if you don't have this magic, bytes have to serialized for transport
1. Pipelining: if your application isn't properly pipelined, shuffles can force a barrier / stop the world->shuffle->restart the world scenario which is very costly
1. Did I mention memory?  Oh, yeah -- well it's going to OOM. Did you think about spilling ?

Now that we put the fear of Knuth in you, let's proceed with an excellent implementation of shuffling

## RAPIDSMPF Shuffling Background

RAPIPDSMPF is a library composed of two large pieces: 
1. a shuffle library designed with spilling / out-of-core memory handling with accelerated transport
1. an actor network for constructing streaming data pipelines 

Because shuffling is hard, slow, memory-intensive, and critical, it's historically where RAPIDMSPF started.  The team set out to build a shuffling library which met the following requirements:

1. Needs to be fast
1. Needs to scale
1. Need to work with larger than VRAM (GPU) data (out-of-core)
1. Need to be resusable

Users today can still adopt *just* the shuffling component of RAPIDSMPF.  (We've seen this adoption in [NeMo-Curator](https://github.com/NVIDIA-NeMo/Curator/blob/15bcdef495246dc98da41954f3a6fb4cc0030a8c/nemo_curator/stages/deduplication/shuffle_utils/rapidsmpf_shuffler.py#L65) and [experimentally in Ray Data](https://github.com/ray-project/ray/blob/90b5e6b993b3fd96f89fd8a2cacf9f3230f4dd7c/python/ray/data/_internal/gpu_shuffle/hash_aggregate.py#L1484))

The docs have nice breakdown on the [specifics of the shuffle implementation](https://docs.rapids.ai/api/rapidsmpf/nightly/background/shuffle-architecture/) including this rendering of three ranks participating in a shuffle

<p><center><img src="rapidsmpf-shuffle-table-fs8.png" alt="rapismpf shuffle"></center></p>


## Benchmarking Setup

cuDF/RapidsMPF has an easy to use C++ benchmark: [bench_shuffle](https://github.com/NVIDIA/cudf/blob/9e8e79962d7ced863e209f49da466a23ec0c5819/cpp/libcudf_streaming/benchmarks/bench_shuffle.cpp) which helps us study how the RapidsMPF implementation works across varied hardware: transports, GPUs, etc, and varied configuration: input/output size, memory resources, etc.

Here's a full breakdown of what the current bench_shuffle test expose to users with some defaults and examples.  Generally speaking, this benchmark builds tunable amounts of random 32-bit (4byte) integers per rank (per GPU), shuffles all the data, and exists

| Flag | Meaning | Values used / seen |
|---|---|---|
| `-C <name>` | Communicator | `ucxx` |
| `-c <n>` | Number of columns | default `1` |
| `-r <n>` | Number of timed runs | `10` |
| `-w <n>` | Number of warmup runs | `3` |
| `-n <n>` | Number of rows per rank | `268435456` (sized for 1 GiB/partition at 4 bytes/row, c=1) |
| `-p <n>` | Number of input partitions per rank | `20` (20 GiB/rank runs), `40` (40 GiB/rank runs) |
| `-o <n>` | Number of output partitions per rank | `8` (20 GiB/rank), `5` then `16` (40 GiB/rank — raised to avoid ~2^31 row/column overflow) |
| `-m <name>` | RMM memory resource | default `pool` |
| `-l <n>` | Device memory limit in MiB | `20480` / `40960` when explicitly set; omitted = unlimited (binary default `-1`) |
| `-s` | Enable output discard (simulate streaming) | flag, always set |
| `-x` | Enable memory profiling | flag, always set |
| `-g` | Use pre-partitioned input tables | flag, always set |

For all tests we are going to use nodes on an NVL72 and we are going to use [rrun](https://github.com/rapidsai/rapidsmpf/tree/2d9a3f2876174514086e780929f6c4d4976c0d2c/cpp/tools), an mpi like multiprocess launch tool capable of binding processes to NUMA nodes.

### Simple Shuffling:


To start let's run on a single NVL4, which has 4 GPUs: 192GB/rank.   

>  rrun -n 4 --bind-to cpu --bind-to memory -x UCX_MAX_RNDV_RAILS=1 -x UCX_PROTO_ENABLE=y -x UCX_WARN_UNUSED_ENV_VARS=n libcudf_streaming_bench_shuffle -C ucxx -w 3 -r 10 -m pool -g -s -x -p 1 -o 4 -c 10 -n 536870912

Here we are *warming* up the benchmark 3 times, then *running* the benchmark 10 times. There are 536_870_912 rows (-n), 10 columns (-c), 1 input partition per rank (-p), and the data will be shuffled into 4 output partitions (-o)

> 536_870_912 rows * 4 bytes (32-bit ints) = 2 GiB partitions
> 32 input partitions * 1 GiB = 32 GiB / rank
> 4 ranks * 20 GiB = 80 GiB total

```bash
# exmaple output for 2GiB partitions, 20GiB/rank
[0:PRINT:0:2026-08-26 19:24:38.920662914] elapsed: 49.45 ms | local throughput: 404.46 GiB/s | global throughput: 1.58 TiB/s
[3:PRINT:0:2026-08-26 19:24:38.920669954] elapsed: 43.85 ms | local throughput: 456.08 GiB/s | global throughput: 1.78 TiB/s
[1:PRINT:0:2026-08-26 19:24:38.920680290] elapsed: 49.50 ms | local throughput: 404.01 GiB/s | global throughput: 1.58 TiB/s
[2:PRINT:0:2026-08-26 19:24:38.920676194] elapsed: 45.30 ms | local throughput: 441.49 GiB/s | global throughput: 1.72 TiB/s
[0:PRINT:0:2026-08-26 19:24:39.006007576] elapsed: 47.23 ms | local throughput: 423.43 GiB/s | global throughput: 1.65 TiB/s
[3:PRINT:0:2026-08-26 19:24:39.006015480] elapsed: 38.50 ms | local throughput: 519.52 GiB/s | global throughput: 2.03 TiB/s
[2:PRINT:0:2026-08-26 19:24:39.006018648] elapsed: 49.47 ms | local throughput: 404.28 GiB/s | global throughput: 1.58 TiB/s
[1:PRINT:0:2026-08-26 19:24:39.006025208] elapsed: 49.47 ms | local throughput: 404.29 GiB/s | global throughput: 1.58 TiB/s
```

Each rank posts how much time it spent shuffling, and the local and global throughput.  Already we can observe that warming up has some cost as it runs slower than the "official" run.  At the end of a run, the program provides the following helpful summarization per rank:

```bash
[0:PRINT:0:2026-08-26 19:24:39.432908004] means: 48.83 ms | local throughput: 409.62 GiB/s | global throughput: 1.60 TiB/s | in_parts: 1 | out_parts: 4 | nranks: 4 | device memory peak: 60 GiB | device memory total: 99.02 GiB (avg)
[3:PRINT:0:2026-08-26 19:24:39.432909156] means: 42.97 ms | local throughput: 465.49 GiB/s | global throughput: 1.82 TiB/s | in_parts: 1 | out_parts: 4 | nranks: 4 | device memory peak: 60 GiB | device memory total: 99.01 GiB (avg)
[1:PRINT:0:2026-08-26 19:24:39.432916516] means: 49.42 ms | local throughput: 404.68 GiB/s | global throughput: 1.58 TiB/s | in_parts: 1 | out_parts: 4 | nranks: 4 | device memory peak: 60 GiB | device memory total: 99.02 GiB (avg)
[2:PRINT:0:2026-08-26 19:24:39.432918372] means: 46.34 ms | local throughput: 431.55 GiB/s | global throughput: 1.69 TiB/s | in_parts: 1 | out_parts: 4 | nranks: 4 | device memory peak: 60 GiB | device memory total: 99.02 GiB (avg)

[0:PRINT:0:2026-08-26 19:24:39.432975652] Statistics (of the last run):
 - alloc-device:                                       15 GiB | 41.72 us | 351.08 TiB/s | avg-stream-delay 14.90 us
 - event-loop-total:                                   1.64 ms | avg 1.99 us
 - metadata-payload-exchange-complete-data-transfers:  180.77 us | avg 219.65 ns
 - metadata-payload-exchange-progress:                 1.11 ms | avg 1.35 us
 - metadata-payload-exchange-receive-metadata:         272.90 us | avg 331.59 ns
 - metadata-payload-exchange-send-messages:            205.79 us | avg 102.90 us
 - metadata-payload-exchange-setup-data-receives:      196.38 us | avg 238.62 ns
 - shuffle-payload-recv:                               15 GiB | avg 1.25 GiB
 - shuffle-payload-send:                               15 GiB | avg 1.25 GiB

Memory Profiling
----------------
Ordered by: peak (descending)
  ncalls        peak      g-peak       accum         max  filename:lineno(name)
       1      60 GiB      60 GiB    1.26 TiB       2 GiB  main (all allocations using RmmResourceAdaptor)
       1      40 GiB      40 GiB   44.02 GiB       2 GiB  /libcudf_streaming/src/partition_utils.cpp:83(partition_and_pack)
       1      20 GiB      20 GiB      20 GiB    1.25 GiB  /libcudf_streaming/src/partition_utils.cpp:110(split_and_pack)
       4       5 GiB       5 GiB      20 GiB  512.17 MiB  /libcudf_streaming/src/partition_utils.cpp:136(unpack_and_concat)
       1         0 B         0 B         0 B         0 B  /libcudf_streaming/benchmarks/bench_shuffle.cpp:276(shuffling)
```

We have the average values per rank: local/global throughput, input/output partitions, etc, and, aggregate statistics for the run per rank (in the above I only list rank 0. ranks 2-3 are very similar): how much data was allocated on the device, how much data was sent through the shuffler, etc.  Given we have 768GB of VRAM per node, RapidsMPF has plenty of room to shuffle without needing to spill.  In the above configuration for data: shape, size RapidsMPF can drive 1.7 TiB/s - 1.8 TiB/s.  We still aren't at the [theoretical ceiling](https://www.nvidia.com/en-us/data-center/gb200-nvl72/) of 7.8TiB/s but that's very fast! When doing shuffles are part of a workflow data comes in a variety of different partition size, shapes, GPUs vary in the amount of VRAM so we should expect throughput to change as the shape and size of the data as well as the workflow overlall changes.  

## Oops, you spilled a little...

Let's apply some memory pressure to the benchamrk and limit the device to 32GB: `-l 32768` and see what happens:

```bash
[3:PRINT:0:2026-08-26 19:24:55.544272082] means: 88.55 ms | local throughput: 225.86 GiB/s | global throughput: 903.44 GiB/s | in_parts: 1 | out_parts: 4 | nranks: 4 | device memory peak: 60 GiB | device memory total: 99.01 GiB (avg)
[3:PRINT:0:2026-08-26 19:24:55.544330866] Statistics (of the last run):
 - alloc-device:                                       15 GiB | 15.64 ms | 959.15 GiB/s | avg-stream-delay 15.48 us
 - alloc-pinned_host:                                  3.75 GiB | 159.26 us | 22.99 TiB/s | avg-stream-delay 16.53 us
 - copy-pinned_host-to-device:                         3.75 GiB | 27.79 ms | 134.95 GiB/s | avg-stream-delay 5.21 ms
 - event-loop-total:                                   3.33 ms | avg 2.38 us
 - metadata-payload-exchange-complete-data-transfers:  236.58 us | avg 169.10 ns
 - metadata-payload-exchange-progress:                 2.70 ms | avg 1.93 us
 - metadata-payload-exchange-receive-metadata:         1.40 ms | avg 997.31 ns
 - metadata-payload-exchange-send-messages:            165.86 us | avg 82.93 us
 - metadata-payload-exchange-setup-data-receives:      339.84 us | avg 242.92 ns
 - shuffle-payload-recv:                               15 GiB | avg 1.25 GiB
 - shuffle-payload-send:                               15 GiB | avg 1.25 GiB
 
Memory Profiling
----------------
Ordered by: peak (descending)
  ncalls        peak      g-peak       accum         max  filename:lineno(name)
       1      60 GiB      60 GiB    1.26 TiB       2 GiB  main (all allocations using RmmResourceAdaptor)
       1      40 GiB      40 GiB   44.02 GiB       2 GiB  /libcudf_streaming/src/partition_utils.cpp:83(partition_and_pack)
       1      20 GiB      20 GiB      20 GiB    1.25 GiB  /libcudf_streaming/src/partition_utils.cpp:110(split_and_pack)
       4       5 GiB       5 GiB      20 GiB  512.17 MiB  /libcudf_streaming/src/partition_utils.cpp:136(unpack_and_concat)
       1         0 B         0 B         0 B         0 B  /libcudf_streaming/benchmarks/bench_shuffle.cpp:276(shuffling)
```

Global throughput has diminsed slightly to `900 GiB/s` and we have two new lines in the statisics: `alloc-pinned_host` and `copy-pinned_host-to-device` but we *dont'* see any line like `copy-device-to-host`.  We are still shuffling the same amount of data: each rank has 20GBs, they receive 15GBs from the other 3 ranks, and at a limit of 32GBs each rank is now are over the limit. However! As RapidsMPF is moving data around, the receive side can observe the memory the pressure and knows about the limit.  Instead of thrashing and moving data back and forth several times D->H/H->D/and back again, the receieve side can accept buffers on the host instead of the device to avoid the thrashing -- thanks UCXX!  

<p><center><img src="shuffle-pipeline.png" alt="rapismpf shuffle"></center></p>

Below is a table sweeping the same initial setup but continually increasing the spill limit:

#### Scaled Spilling NVL4

| Label | Spill limit | Input/rank | Peak device | Local perf | Global throughput | copy-device-to-pinned_host | time | copy-pinned_host-to-device | time |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| no-spill |  ∞ GiB | 20 GiB | 60.0 GiB | 409.6 GiB/s | 1.60 TiB/s | 0 GiB | 0 ms | 0 GiB | 0 ms |
| onset-spill | 32 GiB | 20 GiB | 60.0 GiB | 234.0 GiB/s | 935.9 GiB/s | 0 GiB | 0 ms | 3.8 GiB | 27.8 ms |
| light-spill | 28 GiB | 20 GiB | 60.0 GiB | 166.0 GiB/s | 663.9 GiB/s | 0 GiB | 0 ms | 6.2 GiB | 60.7 ms |
| moderate-spill | 24 GiB | 20 GiB | 60.0 GiB | 127.2 GiB/s | 508.6 GiB/s | 0 GiB | 0 ms | 11.2 GiB | 111.8 ms |
| heavy-spill | 20 GiB | 20 GiB | 60.0 GiB | 92.9 GiB/s | 371.6 GiB/s | 1.2 GiB | 7.3 ms | 15.0 GiB | 179.8 ms |
| very-heavy-spill | 16 GiB | 20 GiB | 60.0 GiB | 78.5 GiB/s | 313.9 GiB/s | 5.0 GiB | 42.2 ms | 16.2 GiB | 203.5 ms |
| extreme-spill | 12 GiB | 20 GiB | 60.0 GiB | 63.9 GiB/s | 255.5 GiB/s | 10.0 GiB | 88.8 ms | 17.5 GiB | 250.7 ms |
