#!/bin/bash
# run_server.sh — start the ucx_perftest server on one node.
#
# Usage:
#   bash run_server.sh <host> <conda_env> [UCX_TLS_value]
#
# Examples:
#   bash run_server.sh <server_ip> cudf-polars                 # SRD/EFA (default, auto-selected)
#   bash run_server.sh <server_ip> cudf-polars tcp,cuda_copy,cuda_ipc,sm,self   # force TCP, exclude srd
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HOST="${1:?host}"
ENV_NAME="${2:?conda env name}"
TLS="${3:-}"

BIN="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ucx_perftest"

TLS_PREFIX=""
[[ -n "$TLS" ]] && TLS_PREFIX="UCX_TLS=${TLS} "

echo "==> [server:$HOST] UCX_TLS=${TLS:-<unset, auto-selects srd/EFA>}"
ssh_run "$HOST" "
rm -f ~/ucx_server.log
setsid bash -c '${TLS_PREFIX}${BIN} -m cuda -t tag_bw -n 10 -s \$((1024*1024*100)) > ~/ucx_server.log 2>&1 < /dev/null &'
sleep 2
pgrep -af ucx_perftest || echo 'WARNING: server process not found'
"
