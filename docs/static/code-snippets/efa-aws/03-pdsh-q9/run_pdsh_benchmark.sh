#!/bin/bash
# run_pdsh_benchmark.sh — run the cudf-polars PDS-H streaming benchmark
# against an already-running Ray cluster (head + workers), from the head
# node, and pull the results back locally.
#
# Usage:
#   bash run_pdsh_benchmark.sh <head_pub_ip> <head_priv_ip> <query> <conda_env> <dataset_path>
#
# Example:
#   bash run_pdsh_benchmark.sh <head_ip> <head_priv_ip> 9 cudf-polars s3://your-bucket/pdsh/scale-1000
#
# UCX_TLS: export before calling to override transport (e.g. to exclude
# srd/EFA and force tcp). Left unset, UCX auto-selects srd as normal.
# UCX_TCP_TUNED=1: apply a tuned plain-TCP UCX config (see common.sh) for
# a fair comparison against TCP's out-of-the-box defaults.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HEAD_PUB="${1:?head public ip}"
HEAD_PRIV="${2:?head private ip}"
QUERY="${3:?query, e.g. 9 or 1,9}"
ENV_NAME="${4:?conda env name}"
DATASET_PATH="${5:?dataset s3 path, e.g. s3://your-bucket/pdsh/scale-1000}"

PY="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/python"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REMOTE_OUT="pdsh_${TIMESTAMP}.txt"
REMOTE_JSON="pdsh_${TIMESTAMP}.jsonl"
LOCAL_OUT="pdsh_${TIMESTAMP}.txt"
LOCAL_JSON="pdsh_${TIMESTAMP}.jsonl"

echo "==> [head:$HEAD_PUB] running pdsh query=${QUERY} dataset=${DATASET_PATH} connect=${HEAD_PRIV}:6379"

ssh_run "$HEAD_PUB" "
cd ~
$AWS_CREDS_SNIPPET
$CUDF_POLARS_ENV_SNIPPET
${UCX_TLS:+export UCX_TLS=${UCX_TLS}}
$([[ "${UCX_TCP_TUNED:-0}" == "1" ]] && echo "$UCX_TCP_TUNING_SNIPPET")

$PY -m cudf_polars.streaming.benchmarks.pdsh ${QUERY} \
    --iterations 2 \
    --path '${DATASET_PATH}' \
    --validate-directory '${DATASET_PATH}/expected/' \
    --output '${REMOTE_JSON}' \
    --suffix '/*.parquet' \
    --frontend ray \
    --connect '${HEAD_PRIV}:6379' \
    --pinned-memory \
    --pinned-initial-pool-size 68719476736 \
    --explain \
    --explain-partition-plan \
    2>&1 | tee '${REMOTE_OUT}'
"

echo "==> fetching results to $(pwd)/"
scp_get "$HEAD_PUB" "~/${REMOTE_OUT}" "$LOCAL_OUT" || true
scp_get "$HEAD_PUB" "~/${REMOTE_JSON}" "$LOCAL_JSON" || true

echo "==> done. local files:"
echo "    $LOCAL_OUT"
echo "    $LOCAL_JSON"
