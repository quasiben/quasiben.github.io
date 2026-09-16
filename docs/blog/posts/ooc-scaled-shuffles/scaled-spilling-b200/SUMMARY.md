# Scaled Spilling NVL8 (DGX B200, 8x B200)

Reproduction of the yabas blog's "Scaled Spilling NVL4" study, scaled to 8 ranks on a
single DGX B200 node (`umb-b200-220`). Same per-rank data volume (20 GiB/rank, 10
columns, `-n 536870912`), same benchmark flags, `-o 8` (one output partition per rank).
Raw logs are the `single-dgxb200-spill-study-*.log` files in this directory.

**Important hardware difference from the original NVL4 study:** the original run was on
a GB200 (`presto-gb200-gcn-04`), a Grace-Blackwell superchip where CPU and GPU share
memory over NVLink-C2C. This machine (`umb-b200-220`) is a conventional x86 DGX B200
(dual Intel Xeon Platinum 8570) with GPUs attached over **PCIe Gen5**. Host-side spill
buffers (pinned host memory) therefore move at very different rates on the two
machines -- see the D2H/H2D rate columns below.

### NVL4 (GB200, NVLink-C2C host) -- original study, for comparison

| Label | Spill limit | Peak device | Local perf | Global throughput | D2H (pinned) vol / time / rate | H2D (pinned) vol / time / rate |
|---|---:|---:|---:|---:|---:|---:|
| no-spill | ∞ GiB | 60.0 GiB | 409.6 GiB/s | 1.60 TiB/s | 0 | 0 |
| onset-spill | 32 GiB | 60.0 GiB | 234.0 GiB/s | 935.9 GiB/s | 0 | 3.8 GiB / 20.9 ms / 179.0 GiB/s |
| light-spill | 28 GiB | 60.0 GiB | 166.0 GiB/s | 663.9 GiB/s | 0 | 5.0 GiB / 35.8 ms / 139.7 GiB/s |
| moderate-spill | 24 GiB | 60.0 GiB | 127.2 GiB/s | 508.6 GiB/s | 0 | 11.2 GiB / 111.8 ms / 100.6 GiB/s |
| heavy-spill | 20 GiB | 60.0 GiB | 92.9 GiB/s | 371.6 GiB/s | 1.2 GiB / 7.3 ms / 171.2 GiB/s | 15.0 GiB / 175.2 ms / 85.6 GiB/s |
| very-heavy-spill | 16 GiB | 60.0 GiB | 78.5 GiB/s | 313.9 GiB/s | 5.0 GiB / 42.2 ms / 118.4 GiB/s | 16.2 GiB / 199.7 ms / 81.4 GiB/s |
| extreme-spill | 12 GiB | 60.0 GiB | 63.9 GiB/s | 255.5 GiB/s | 10.0 GiB / 88.8 ms / 112.6 GiB/s | 17.5 GiB / 250.7 ms / 69.8 GiB/s |

### NVL8 (DGX B200, PCIe Gen5 host) -- this study

| Label | Spill limit | Peak device | Local perf | Global throughput | D2H (pinned) vol / time / rate | H2D (pinned) vol / time / rate |
|---|---:|---:|---:|---:|---:|---:|
| no-spill | ∞ GiB | 60.0 GiB | 229.7 GiB/s | 1.79 TiB/s | 0 | 0 |
| onset-spill | 32 GiB | 60.0 GiB | 60.0 GiB/s | 480.2 GiB/s | 0 | 5.6 GiB / 191.4 ms / 29.4 GiB/s |
| light-spill | 28 GiB | 60.0 GiB | 35.9 GiB/s | 287.0 GiB/s | 0 | 9.7 GiB / 379.0 ms / 25.6 GiB/s |
| moderate-spill | 24 GiB | 60.0 GiB | 26.5 GiB/s | 211.7 GiB/s | 0 | 13.8 GiB / 491.0 ms / 28.0 GiB/s |
| heavy-spill | 20 GiB | 60.0 GiB | 14.8 GiB/s | 118.6 GiB/s | 0.3 GiB / 9.6 ms / 32.6 GiB/s | 17.5 GiB / 622.3 ms / 28.1 GiB/s |
| very-heavy-spill | 16 GiB | 60.0 GiB | 10.1 GiB/s | 80.5 GiB/s | 4.1 GiB / 128.7 ms / 31.6 GiB/s | 18.1 GiB / 1340.0 ms / 13.5 GiB/s |
| extreme-spill | 12 GiB | 60.0 GiB | 6.9 GiB/s | 55.5 GiB/s | 8.1 GiB / 264.5 ms / 30.7 GiB/s | 18.8 GiB / 1170.0 ms / 16.1 GiB/s |

### Reading the rate columns

- **NVL4 (GB200) H2D rate**: 179 -> 69.8 GiB/s as spill deepens -- fast throughout,
  because pinned-host refills ride NVLink-C2C, which is shared with basically nothing
  else on the node.
- **NVL8 (this machine) H2D rate**: 29.4 -> 13.5-16.1 GiB/s -- capped from the very
  first spill step at roughly PCIe Gen5's practical per-link ceiling, and it gets
  *worse* (not just proportionally slower) at very-heavy/extreme, where 8 ranks are all
  doing D2H eviction and H2D refill concurrently and contending for the same host
  memory/PCIe fabric shared across two NUMA sockets.
- Same qualitative story on `copy-device-to-pinned_host`: 0 GiB through `moderate-spill`
  on both machines (receive-side host buffering avoids eviction until the limit reaches
  the 20 GiB input size) -- the *mechanism* RapidsMPF uses is identical, only the
  *bandwidth available to it* differs by hardware.

This explains why NVL8's onset-spill throughput (480 GiB/s, 27% of baseline) doesn't
land near the ~1 TiB/s (58% of baseline) seen at NVL4's onset-spill: it's not a
misconfigured sweep, it's PCIe Gen5 vs NVLink-C2C for the host spill path.
