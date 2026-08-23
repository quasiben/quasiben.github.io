#!/bin/bash
# ray_start_worker.sh — start (or restart) a Ray worker node on a remote
# host, joining an existing head.
#
# Usage: bash ray_start_worker.sh <worker_pub_ip> <head_priv_ip> <conda_env>
#
# UCX_TLS: export before calling to override transport (e.g. to exclude
# srd/EFA and force tcp). Left unset, UCX auto-selects srd as normal.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

WORKER_PUB="${1:?worker public ip}"
HEAD_PRIV="${2:?head private ip}"
ENV_NAME="${3:?conda env name}"

NGPU=$(gpu_count "$WORKER_PUB")
echo "==> [worker:$WORKER_PUB] GPUs detected: $NGPU" >&2

RAY="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ray"

echo "==> [worker:$WORKER_PUB] stopping any existing ray processes" >&2
ssh_run "$WORKER_PUB" "$RAY stop --timeout 30" >&2 || ssh_run "$WORKER_PUB" "$RAY stop --force" >&2 || true
sleep 2

echo "==> [worker:$WORKER_PUB] ensuring efa_nv_peermem is loaded" >&2
ensure_efa_peermem "$WORKER_PUB" >&2

UCX_TLS_EXPORT=""
if [[ -n "${UCX_TLS:-}" ]]; then
    UCX_TLS_EXPORT="export UCX_TLS=${UCX_TLS}"
fi

echo "==> [worker:$WORKER_PUB] joining head at ${HEAD_PRIV}:6379 (num-gpus=$NGPU, UCX_TLS=${UCX_TLS:-<unset>})" >&2
ssh_run "$WORKER_PUB" "
$AWS_CREDS_SNIPPET
$CUDF_POLARS_ENV_SNIPPET
$UCX_TLS_EXPORT
rm -f ~/ray_worker.log
setsid $RAY start --address=${HEAD_PRIV}:6379 --num-gpus=${NGPU} \
    > ~/ray_worker.log 2>&1 < /dev/null &
disown
"

echo "==> [worker:$WORKER_PUB] waiting for worker to join" >&2
for _ in $(seq 1 30); do
    if ssh_run "$WORKER_PUB" "grep -qi 'Ray runtime started\|Connected to Ray cluster' ~/ray_worker.log 2>/dev/null"; then
        break
    fi
    sleep 2
done
if ! ssh_run "$WORKER_PUB" "grep -qi 'Ray runtime started\|Connected to Ray cluster' ~/ray_worker.log 2>/dev/null"; then
    echo "ERROR: worker did not join within timeout. Log:" >&2
    ssh_run "$WORKER_PUB" "cat ~/ray_worker.log" >&2
    exit 1
fi
echo "==> [worker:$WORKER_PUB] joined cluster" >&2
