---
title: Faster Transport on Cloud Infra
date: 2026-08-24
author: Benjamin Zaitlen
slug: efa-aws
---

**EFA's SRD transport moves CUDA buffers ~40x faster than tuned TCP, ~13x faster through a shuffle, and ~2x faster using PDS-H Q9 as a benchmark. Why is there a performance difference between SRD and TCP, and why does that difference shrink as workload complexity grows?**

My team and I have been evaluating cuDF-Polars in cloud deployments and we've been experimenting with EFA enabled nodes. EFA is a high-performance network interface that allows for low-latency, high-bandwidth communication between nodes. Importantly, EFA enables GPU-to-GPU communication across nodes which is a great fit for our use case. Generally, this is referred to as GPUDirect RDMA (Remote Direct Memory Access) which is a key component of distributed high-performance computing. Tools like NCCL, UCX, NIXL with cuda_copy or gdrcopy capabilities have built-in support for doing this kind of transfer. In this blog post, I'll mostly be recording configuration and testing details for EFA enabled nodes.


AWS has a complete [list of EFA enabled instance types](https://docs.aws.amazon.com/ec2/latest/instancetypes/ac.html#ac_network) and they also provide [launch templates](https://docs.aws.amazon.com/pcs/latest/userguide/working-with_networking_efa_create-lt.html) for easy consumption (human or agent). Note: I don't think one can enable or turn on proper network adapters through the UI. Much of this work is also summarized in a helpful [getting started page](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html) though UCX is not listed in the test setup so I'll write it down here. It's a little buried, but on the [g7e aws](https://aws.amazon.com/ec2/instance-types/g7e/) page, AWS notes that GPUDirect RDMA is only supported for multi-GPU instances. So, every experiment below runs on two g7e.12xlarge nodes: 2 GPUs (Blackwell PRO, 48 vCPUs) per node or 4 GPUs / 4 ranks total.

> You can find all run and launch scripts in the companion folder: *[EFA benchmark scripts](/static/code-snippets/efa-aws/)*. These scripts drive both nodes over SSH from your laptop.

I get a little confused with so much jargon and similar acronyms but this is what I've boiled accelerated networking on AWS down to:

1. ENA = standard networking
1. ENA Express = better ENA with SRD (Scalable Reliable Datagram) for low latency
1. EFA = RDMA with SRD and only some instances support GPUDirect RDMA 


I'm using an Ubuntu image; official AWS images may already have the modules baked in. To launch with EFA enabled we need the following: 

1. Launch a node with EFA enabled: `InterfaceType=efa`
1. Setup EFA on the node:

```bash
echo "==> Installing AWS EFA software stack"
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
tar -xzf aws-efa-installer-latest.tar.gz
cd aws-efa-installer
sudo ./efa_installer.sh -y --skip-limit-conf
cd ..
rm -rf aws-efa-installer aws-efa-installer-latest.tar.gz

echo "Enabling efa_nv_peermem kernel module (required for CUDA)"
sudo modprobe efa_nv_peermem

echo "Enabling persistent loading of efa_nv_peermem kernel module"
echo "efa_nv_peermem" | sudo tee /etc/modules-load.d/efa_nv_peermem.conf
```

*[Script: 00-launch](/static/code-snippets/efa-aws/00-launch/launch_efa_nodes.sh)* does all of the above unattended.


You can verify a working EFA setup with the following:

```bash
ubuntu@ip-172-31-29-167:~$ fi_info -p efa   # should list efa_0 device
provider: efa
    fabric: efa-direct
    domain: rdmap47s0-rdm
    version: 206.0
    type: FI_EP_RDM
    protocol: FI_PROTO_EFA
provider: efa
    fabric: efa
    domain: rdmap47s0-rdm
    version: 206.0
    type: FI_EP_RDM
    protocol: FI_PROTO_EFA
provider: efa
    fabric: efa
    domain: rdmap47s0-dgrm
    version: 206.0
    type: FI_EP_DGRAM
    protocol: FI_PROTO_EFA
ubuntu@ip-172-31-29-167:~$ ls /dev/infiniband/uverbs*  # should exist
/dev/infiniband/uverbs0
```


## UCX Performance

*[Scripts: 01-ucx-perftest](/static/code-snippets/efa-aws/01-ucx-perftest/run_client.sh)*

I first want to validate EFA with UCX.


Check UCX can "see" srd:

```bash
(cudf-polars) ubuntu@ip-172-31-29-167:~$ ucx_info -d | grep srd
#      Transport: srd
```

Now we can run `ucx_perftest` which will measure bandwidth as we send 100 MiB GPU buffers across the wire in one direction:

> Note: I needed to tune TCP configs a bit to get more reasonable output. Out of the box, UCX defaults don't prioritize TCP transport.

### Example output with SRD

```bash
# On Node A
ucx_perftest -m cuda -t tag_bw -n 10 -s $((1024*1024*100))

# On Node B
ucx_perftest -m cuda -t tag_bw -n 10 -s $((1024*1024*100)) IP_NODE_A

+--------------+--------------+------------------------------+---------------------+-----------------------+
|              |              |       overhead (usec)        |   bandwidth (MB/s)  |  message rate (msg/s) |
+--------------+--------------+----------+---------+---------+----------+----------+-----------+-----------+
|    Stage     | # iterations | 50.0%ile | average | overall |  average |  overall |  average  |  overall  |
+--------------+--------------+----------+---------+---------+----------+----------+-----------+-----------+
Final:                    10      0.353  2202.916  2202.916    45394.37   45394.37         454         454
```

### Example output without SRD

```bash
UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self \
UCX_TCP_TX_SEG_SIZE=256K UCX_TCP_RX_SEG_SIZE=256K UCX_TCP_MAX_BW=auto \
UCX_TCP_SNDBUF=4M UCX_TCP_RCVBUF=4M \
ucx_perftest -m cuda -t tag_bw -n 10 -s $((1024*1024*100)) IP_NODE_A

+--------------+--------------+------------------------------+---------------------+-----------------------+
|              |              |       overhead (usec)        |   bandwidth (MB/s)  |  message rate (msg/s) |
+--------------+--------------+----------+---------+---------+----------+----------+-----------+-----------+
|    Stage     | # iterations | 50.0%ile | average | overall |  average |  overall |  average  |  overall  |
+--------------+--------------+----------+---------+---------+----------+----------+-----------+-----------+
Final:                    10      0.568 87966.013 87966.013     1136.80    1136.80          11          11
```

### UCX Results

| Transport | Bandwidth | Message rate | Average overhead |
|---|---|---|---|
| SRD (EFA)          | **45.39 GB/s** | **454 msg/s** | **2.20 ms** |
| TCP, tuned (`srd` excluded) | 1.14 GB/s  | 11 msg/s       | 87.97 ms |

With SRD/EFA, UCX can transfer CUDA buffers at `~45GB/s`, and without SRD (TCP only) bandwidth is severely degraded to `~1.1GB/s`, a ~40x performance difference. TCP being slow here is expected and conversely demonstrates why GPU RDMA is critical. With TCP, GPU data is moved from device to host (D->H), serialized, then sent across the wire, then deserialized, and finally moved back from host to device (H->D). GPU RDMA avoids the costly H->D / D->H movement and the serialization costs. (Hmm, it's the same NIC between TCP/SRD. Perhaps a question for later). Effectively, with GPU RDMA, the pipeline is: GPU->NIC->RemoteNIC->RemoteGPU. AWS states that on a g7e.12xlarge the transport is 400Gbps or ~50GB/s, very close to what we measure with `ucx_perftest`.

Let's increase complexity from simple perf testing...


## Shuffle Performance

*[Scripts: 02-shuffle-bench](/static/code-snippets/efa-aws/02-shuffle-bench/run_shuffle_bench.sh)*

In these blogs I haven't gone into much of the underlying machinery of cuDF-Polars but a lot of the performance comes from the [accelerated and out-of-core shuffle](https://docs.rapids.ai/api/rapidsmpf/stable/background/shuffle-architecture/) implemented in RAPIDSMPF. Let's do a similar experiment to `ucx_perftest` where, instead of just shoving bytes across the wire, we measure the bandwidth of a more complex workload, a shuffle, again with and without SRD/EFA. A shuffle is more revealing than point-to-point transfers, and it's more than just an all-to-all. Here every rank is sending and receiving at once and doing some hash calculations as well. In this experiment we configure a benchmark to shuffle 20 GiB of randomly generated data per rank (per GPU). Each 20 GiB is composed of 1 GiB input partitions and is redistributed into 8 output partitions of ~2.5 GiB per rank. The benchmark runs 3 warmups and then shuffles the same data 10 more times, and the number we care about is the local throughput. Below is a simplified example of what was executed:

```bash
# 20 GiB/rank at 1 GiB per input partition, c=1 (default, 4 bytes/row):
#   -n = 1024*1024*1024 / 4 = 268435456 rows -> exactly 1024 MiB/partition
#   -p 20 partitions * 1 GiB = 20 GiB/rank
#   -o 8: number of output partitions

# experiment with srd
${CONDA_PREFIX}/bin/libcudf_streaming_bench_shuffle -w 3 -r 10 -g -s -x -n 268435456 -p 20 -o 8

# experiment without srd, TCP tuned (see note below)
UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self \
UCX_TCP_TX_SEG_SIZE=256K UCX_TCP_RX_SEG_SIZE=256K UCX_TCP_MAX_BW=auto \
UCX_TCP_SNDBUF=4M UCX_TCP_RCVBUF=4M \
    ${CONDA_PREFIX}/bin/libcudf_streaming_bench_shuffle -w 3 -r 10 -g -s -x -n 268435456 -p 20 -o 8
```

### Example output with SRD

```bash
 [3:PRINT:0:2026-08-24 15:12:43.805116391] means: 726.79 ms | local throughput: 27.52 GiB/s | global throughput: 110.07 GiB/s | in_parts: 20 | out_parts: 8 | nranks: 4 | device memory peak: 35 GiB | device memory total:
 135.31 GiB (avg)
[2:PRINT:0:2026-08-23 01:37:19.838332753] Statistics (of the last run):
 - alloc-device:                                       15 GiB | 4.42 ms | 3.32 TiB/s | avg-stream-delay 26.23 us
 - event-loop-total:                                   451.58 ms | avg 11.37 us
 - metadata-payload-exchange-complete-data-transfers:  145.69 ms | avg 3.67 us
 - metadata-payload-exchange-progress:                 427.41 ms | avg 10.77 us
 - metadata-payload-exchange-receive-metadata:         229.34 ms | avg 5.78 us
 - metadata-payload-exchange-send-messages:            3.19 ms
 - metadata-payload-exchange-setup-data-receives:      14.75 ms | avg 371.56 ns
 - shuffle-payload-recv:                               15 GiB | avg 32.01 MiB
 - shuffle-payload-send:                               15 GiB | avg 32 MiB
```

### Shuffle Results

| Transport | Mean elapsed/rank | Local throughput | Global throughput |
|---|---|---|---|
| SRD (EFA)          | **727.6 ms**  | **27.52 GiB/s**   | **109.95 GiB/s** |
| TCP, tuned (`srd` excluded) | 9.45 s | 2.13 GiB/s | 8.52 GiB/s |


Again, not unexpected but also so amazing that we have software which can leverage this powerful hardware. `libcudf_streaming_bench_shuffle` isn't hitting max perf of the card but a shuffle is also doing more than just transporting data. Still, as we increased complexity of the workload we can still see the benefits: ~13x perf difference.

## Last One, I Promise: PDS-H Query 9

*[Scripts: 03-pdsh-q9](/static/code-snippets/efa-aws/03-pdsh-q9/run_cluster_benchmark.sh)*

Ok, one more experiment. Let's increase complexity again and run an end-to-end example. As mentioned at the top, we have been studying cloud performance for real world examples. With that in mind let's run a PDS-H workload, an experimental TPC-H-derived benchmark shipped with cuDF-Polars. Specifically, we will only study query 9, which has several joins and will therefore include many shuffles. PDS-H is not an official or TPC-H-compliant benchmark, so these numbers should not be compared with published TPC-H results.

### Q9 Physical Plan SF1K

```
SORT ('nation', 'o_year') ('nation', 'o_year', 'sum_profit') [120]
  SELECT ('nation', 'o_year', 'sum_profit') [120]
    GROUPBY ('nation', 'o_year') ('nation', 'o_year', '__________0') [120]
      SELECT ('nation', 'o_year', 'amount') [120]
        PROJECTION ('n_name', 'o_orderdate', 'l_extendedprice', 'l_discount', 'ps_supplycost', 'l_quantity') [120]
          JOIN Inner ('s_nationkey',) ('n_nationkey',) ('ps_supplycost', 'l_quantity', 'l_extendedprice', '...', 's_natio
nkey', 'n_name') [120]
            PROJECTION ('ps_supplycost', 'l_quantity', 'l_extendedprice', 'l_discount', 'o_orderdate', 's_nationkey') [12
0]
              JOIN Inner ('l_orderkey',) ('o_orderkey',) ('ps_supplycost', 's_nationkey', 'l_quantity', '...', 'l_orderke
y', 'o_orderdate') [120]
                PROJECTION ('ps_supplycost', 's_nationkey', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_orderkey')
[120]
                  JOIN Inner ('p_partkey', 'ps_suppkey') ('l_partkey', 'l_suppkey') ('ps_supplycost', 's_nationkey', 'p_p
artkey', '...', 'l_extendedprice', 'l_discount') [120]
                    PROJECTION ('ps_supplycost', 's_nationkey', 'p_partkey', 'ps_suppkey') [8]
                      JOIN Inner ('ps_suppkey',) ('s_suppkey',) ('p_partkey', 'ps_suppkey', 'ps_supplycost', 's_nationkey
') [8]
                        JOIN Inner ('p_partkey',) ('ps_partkey',) ('p_partkey', 'ps_suppkey', 'ps_supplycost') [8]
                          PROJECTION ('p_partkey',) [4]
                            STREAMINGSCAN ('p_partkey', 'p_name') [4]
                          STREAMINGSCAN ('ps_suppkey', 'ps_supplycost', 'ps_partkey') [8]
                        STREAMINGSCAN ('s_nationkey', 's_suppkey') [1]
                    STREAMINGSCAN ('l_orderkey', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_partkey', 'l_suppkey')
 [120]
                STREAMINGSCAN ('o_orderdate', 'o_orderkey') [8]
            STREAMINGSCAN ('n_name', 'n_nationkey') [1]
```


In cuDF-Polars, PDS-H is baked in, making it easy to test. Running Q9 at SF1000 on the same two-node, four-GPU cluster:

```bash
python -m cudf_polars.streaming.benchmarks.pdsh ${QUERY}
```

This time the gap is much smaller than the raw bandwidth and shuffle numbers above would suggest:

| Transport | Q9 Iter 0 | Q9 Iter 1 |
| --- | --- | --- |
| SRD (EFA)          | **7.94s** | **5.23s** |
| TCP, tuned (`srd` excluded) | 16.79s  | 14.31s |

Nice! We still observe a healthy performance gain: a 2x-2.7x perf difference comparing iterations.

The gap between the iterations is a little bothersome. The OS isn't caching a file -- we're reading chunks of data from S3. I think there is some cost in spinning up the engine: kvikio threads have a small startup cost and maybe there is some caching of the data *within* S3. 

## Wrapping Up

We started at a 40x raw perf difference comparing SRD/EFA to TCP, then we saw a 13x perf difference when performing a shuffle. Now, with an end-to-end example we see 2x-2.7x perf differences. This is reasonable given that the workload isn't shuffling the entire time. We can see in the physical plan above that Q9 is also pulling data from S3, doing some selections, group-bys, etc. 

Still, 2x is a great win for distributed accelerated data processing, and I'd expect it to be the floor rather than the ceiling. The more data a query moves and the more shuffles it induces, the more of the workload is network-bound -- and that is where SRD/EFA shines and delivers higher bandwidth.


## A Secret Deep Dive: Why Doesn't Tuned TCP Hit Max Perf?

Even after tuning TCP, we still hit a ceiling, and TCP never got close to SRD's throughput. Is this a UCX inefficiency, or a hard limit somewhere lower in the stack? ENA (the underlying NIC on these instances) is capable of the same raw ~400Gbps line rate as EFA. To isolate the transport from UCX entirely, we can evaluate plain `iperf3` between the same two nodes.

### Single stream, varying transfer size

```bash
iperf3 -c <server_priv_ip> -P 1 -n <size>
```

| Transfer size | Average bitrate | Steady-state rate |
|---|---|---|
| 1 GB | 8.58 Gbit/s | ~9.5 Gbit/s |
| 5 GB | 8.59 Gbit/s | 9.52-9.53 Gbit/s |
| 10 GB | 8.59 Gbit/s | 9.52-9.54 Gbit/s |
| 20 GB | 9.04 Gbit/s | 9.52-9.54 Gbit/s |

A single TCP process seems to only be able to drive **~9.53 Gbit/s (~1.19 GB/s)** in every full-second interval, no matter how much data is transferred. Transfer rates can vary: small data typically transfers with a lower bandwidth, large data with a higher bandwidth. Instead of varying the data size, let's increase the number of processes.

### Multi-stream aggregate

```bash
iperf3 -c <server_priv_ip> -t 8 -P 64
```

Here, we are using 64 parallel TCP streams transferring 128MB buffers (the system default). For this experiment, I measured **381 Gbit/s (~47.6 GB/s)**, very close to the 400Gbps AWS advertises. So the full bandwidth is there for the taking, and ENA's raw capacity really is comparable to EFA's (ENA + GPU RDMA). But! The application: `ucx_perftest`, cuDF-Polars, etc, is now responsible for splitting up the data and driving dozens of concurrent transfers over the wire. Much easier said than done.
