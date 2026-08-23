#!/bin/bash
# run_cluster_benchmark.sh — end-to-end: start a Ray head on node A, a Ray
# worker on node B joined to it, run the cudf-polars pdsh benchmark on the
# head, then tear the cluster down.
#
# Usage:
#   bash run_cluster_benchmark.sh <head_pub_ip> <worker_pub_ip> <query> <conda_env> <dataset_path>
#
# Example:
#   bash run_cluster_benchmark.sh <head_ip> <worker_ip> 9 cudf-polars s3://your-bucket/tpch/scale-1000
#
# UCX_TLS: export before calling to override transport (e.g. to exclude
# srd/EFA and force tcp).
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HEAD_PUB="${1:?head public ip (node A)}"
WORKER_PUB="${2:?worker public ip (node B)}"
QUERY="${3:?query, e.g. 9 or 1,9}"
ENV_NAME="${4:?conda env name}"
DATASET_PATH="${5:?dataset s3 path, e.g. s3://your-bucket/tpch/scale-1000}"

cleanup() {
    echo ""
    echo "==> Tearing down ray cluster"
    bash ./03-pdsh-q9/ray_stop.sh "$WORKER_PUB" "$ENV_NAME" || true
    bash ./03-pdsh-q9/ray_stop.sh "$HEAD_PUB" "$ENV_NAME" || true
}
trap cleanup EXIT

echo "==> Step 1/3: starting ray head on $HEAD_PUB"
HEAD_PRIV=$(bash ./03-pdsh-q9/ray_start_head.sh "$HEAD_PUB" "$ENV_NAME" | tail -1)
echo "==> head private ip: $HEAD_PRIV"

echo ""
echo "==> Step 2/3: starting ray worker on $WORKER_PUB, joining $HEAD_PRIV"
bash ./03-pdsh-q9/ray_start_worker.sh "$WORKER_PUB" "$HEAD_PRIV" "$ENV_NAME"

echo ""
echo "==> Cluster status:"
bash ./03-pdsh-q9/ray_status.sh "$HEAD_PUB" "$ENV_NAME" || true

echo ""
echo "==> Step 3/3: running pdsh benchmark (query=${QUERY})"
bash ./03-pdsh-q9/run_pdsh_benchmark.sh "$HEAD_PUB" "$HEAD_PRIV" "$QUERY" "$ENV_NAME" "$DATASET_PATH"

echo ""
echo "==> Benchmark complete."
