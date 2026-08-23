#!/bin/bash
# ray_start_head.sh — start (or restart) a Ray head node on a remote host.
#
# Usage: bash ray_start_head.sh <head_pub_ip> <conda_env>
#
# Prints the head's private IP on success (last line of stdout), so callers
# can capture it with: HEAD_PRIV=$(bash ray_start_head.sh ... | tail -1)
#
# UCX_TLS: export before calling to override transport (e.g. to exclude
# srd/EFA and force tcp). Left unset, UCX auto-selects srd as normal.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HEAD_PUB="${1:?head public ip}"
ENV_NAME="${2:?conda env name}"

NGPU=$(gpu_count "$HEAD_PUB")
echo "==> [head:$HEAD_PUB] GPUs detected: $NGPU" >&2

RAY="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ray"

echo "==> [head:$HEAD_PUB] stopping any existing ray processes" >&2
ssh_run "$HEAD_PUB" "$RAY stop --timeout 30" >&2 || ssh_run "$HEAD_PUB" "$RAY stop --force" >&2 || true
sleep 2

echo "==> [head:$HEAD_PUB] ensuring efa_nv_peermem is loaded" >&2
ensure_efa_peermem "$HEAD_PUB" >&2

UCX_TLS_EXPORT=""
if [[ -n "${UCX_TLS:-}" ]]; then
    UCX_TLS_EXPORT="export UCX_TLS=${UCX_TLS}"
fi

echo "==> [head:$HEAD_PUB] starting ray head (num-gpus=$NGPU, UCX_TLS=${UCX_TLS:-<unset>})" >&2
ssh_run "$HEAD_PUB" "
$AWS_CREDS_SNIPPET
$CUDF_POLARS_ENV_SNIPPET
$UCX_TLS_EXPORT
rm -f ~/ray_head.log
setsid $RAY start --head --num-gpus=${NGPU} --dashboard-host=0.0.0.0 \
    > ~/ray_head.log 2>&1 < /dev/null &
disown
"

echo "==> [head:$HEAD_PUB] waiting for ray head to come up" >&2
for _ in $(seq 1 30); do
    if ssh_run "$HEAD_PUB" "grep -q 'Ray runtime started' ~/ray_head.log 2>/dev/null"; then
        break
    fi
    sleep 2
done
if ! ssh_run "$HEAD_PUB" "grep -q 'Ray runtime started' ~/ray_head.log 2>/dev/null"; then
    echo "ERROR: ray head did not report ready within timeout. Log:" >&2
    ssh_run "$HEAD_PUB" "cat ~/ray_head.log" >&2
    exit 1
fi

HEAD_PRIV=$(private_ip "$HEAD_PUB")
echo "==> [head:$HEAD_PUB] ray head up. private_ip=$HEAD_PRIV port=6379" >&2
echo "$HEAD_PRIV"
