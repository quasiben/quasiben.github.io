# EFA / GPUDirect RDMA benchmark scripts

Companion scripts for the [EFA Configuration and Testing](/blog/efa/)
post. These automate running the same three experiments described there —
`ucx_perftest`, a raw shuffle benchmark, and an end-to-end cuDF-Polars
TPC-H query — across a pair of EFA-enabled, multi-GPU EC2 nodes (e.g.
g7e.12xlarge), each with and without SRD (EFA) transport.

All scripts drive the nodes over SSH from your local machine; nothing
needs to be installed locally beyond `ssh`/`scp`/bash. No credentials are
stored anywhere — AWS credentials are fetched at runtime from each
instance's IAM role via IMDSv2, and SSH access uses your own key.

## Prerequisites

- Two EC2 instances (a multi-GPU EFA-enabled type, e.g. g7e.12xlarge or
  larger — [GPUDirect RDMA over EFA is multi-GPU-only](https://aws.amazon.com/ec2/instance-types/g7e/)),
  in the same subnet/security group, with EFA enabled
  (`InterfaceType=efa` at launch) and the AWS EFA installer + `efa_nv_peermem`
  module available. `00-launch/launch_efa_nodes.sh` (below) does all of this
  for you, or see the main post for the install snippet if you're setting
  up nodes some other way.
- A conda/miniforge environment on both nodes with `cudf-polars`,
  `rapidsmpf`, `ray`, and UCX (with EFA/`srd` support) installed — also
  handled by the launch script.
- An SSH key that can reach both nodes, and an IAM instance role with S3
  read access to your dataset (only needed for the pdsh step).
- `jq` installed on the remote nodes (used by the AWS credentials snippet) —
  the launch script installs it.
- The AWS CLI installed and configured locally (only needed for the launch
  script).

## 0. Launch EFA-enabled nodes

```bash
cd 00-launch

export IAM_PROFILE=your-instance-profile-with-s3-read
export SECURITY_GROUP=sg-xxxxxxxx   # must allow SSH from your IP and all traffic within the group itself
export SUBNET_ID=subnet-xxxxxxxx
export KEY_NAME=your-ec2-key-pair-name

bash launch_efa_nodes.sh
```

Launches 2 `g7e.12xlarge` instances by default (override with
`INSTANCE_TYPE`/`COUNT`), installs CUDA + `jq` + the AWS EFA stack +
Miniforge, builds a `cudf-polars` conda env and a `rapidsmpf` conda env
(built from source) on each, enables `efa_nv_peermem` to load persistently
on boot, and reboots. When it's done it prints the SSH commands, the
`SSH_KEY_PATH`/`AWS_REGION_` exports the benchmark scripts below expect, and
a `--terminate <instance-ids...>` invocation for teardown. See the script
header for all configurable env vars.

Teardown is `--terminate`, not a stop: instances are launched with
`DeleteOnTermination=true`, so their root volumes go away too. `--stop` still
works as a deprecated alias and does the same thing. Unlike the launch path,
teardown needs no configuration beyond the region.

## Configuration

Everything is parameterized through `common.sh` — set these before
running any script, or export them in your shell profile:

```bash
export SSH_KEY_PATH=~/.ssh/your_key.pem   # default: ~/.ssh/id_ed25519
export REMOTE_USER=ubuntu                 # default: ubuntu
export AWS_REGION_=us-east-2              # default: us-east-2
```

## 1. `ucx_perftest` — raw point-to-point bandwidth

```bash
cd 01-ucx-perftest

# SRD/EFA (default)
bash run_server.sh <server_ip> <conda_env>
bash run_client.sh <client_ip> <server_private_ip> <conda_env>

# TCP only (srd excluded)
bash run_server.sh <server_ip> <conda_env> tcp,cuda_copy,cuda_ipc,sm,self
bash run_client.sh <client_ip> <server_private_ip> <conda_env> tcp,cuda_copy,cuda_ipc,sm,self
```

The client prints the negotiated transport (`srd/rdmap...` vs
`tcp/...`) and the final bandwidth line.

## 2. Shuffle benchmark — `libcudf_streaming_bench_shuffle`

Launched via `mpirun` across both nodes (not `rrun` — that tool only
supports single-node or Slurm-scheduled multi-node launches). GPU/NUMA/core
binding per rank is handled by [`binder.sh`](https://github.com/LStuber/binding)
(MIT licensed, included here unmodified), which binds one MPI rank per GPU.

```bash
cd 02-shuffle-bench

# one-time per node pair: lets mpirun on node A SSH out to node B
bash setup_mpi_ssh.sh <node_a_ip> <node_b_ip>

# SRD/EFA (default — UCX_TLS left unset)
bash run_shuffle_bench.sh <node_a_ip> <node_b_ip> <conda_env>

# TCP only (srd excluded)
UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self \
    bash run_shuffle_bench.sh <node_a_ip> <node_b_ip> <conda_env>
```

Default sizing is 20 GiB/rank (1 GiB input partitions x 20, redistributed
into 8 output partitions), 3 warmup + 10 timed runs. Override by appending
extra `libcudf_streaming_bench_shuffle` flags after `<conda_env>`.

## 3. End-to-end: cuDF-Polars PDS-H query over Ray

```bash
cd 03-pdsh-q9

bash run_cluster_benchmark.sh <head_ip> <worker_ip> 9 <conda_env> s3://your-bucket/tpch/scale-1000

UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self \
    bash run_cluster_benchmark.sh <head_ip> <worker_ip> 9 <conda_env> s3://your-bucket/tpch/scale-1000
```

This starts a Ray head + worker (one per node), runs the query via
`cudf_polars.streaming.benchmarks.pdsh --frontend ray`, pulls the result
files back locally, and tears the cluster down afterward (always, even on
failure, via a `trap`).

Individual steps (`ray_start_head.sh`, `ray_start_worker.sh`,
`ray_status.sh`, `ray_stop.sh`, `run_pdsh_benchmark.sh`) can also be run on
their own if you want to keep a cluster up between multiple queries.

## Notes carried over from the original investigation

- **`efa_nv_peermem` must be loaded before any CUDA memory registration
  happens**, on every node, every boot. If it isn't, UCX silently and
  permanently falls back to slow host-staged transfers for that process's
  lifetime — no error, no warning. All scripts here call
  `ensure_efa_peermem` before starting anything that touches CUDA memory.
- **Stop worker processes gracefully, not with `SIGKILL`.** Force-killing
  Ray/MPI workers doesn't give them a chance to cleanly deregister GPU
  memory from the EFA device, which leaves it in a degraded state
  (`dmesg` shows repeated `DEREG_MR ... err -22`) that silently corrupts
  the results of later runs. `ray_stop.sh` here tries a graceful stop
  first and only falls back to `--force` if that times out.
- `RAPIDSMPF_UCXX_PROGRESS_MODE=thread-polling` (set in `common.sh`) is
  the single biggest lever found for shuffle-heavy workloads — the
  default (`thread-blocking`) leaves several times the performance on the
  table. See the main post for the full before/after numbers.
