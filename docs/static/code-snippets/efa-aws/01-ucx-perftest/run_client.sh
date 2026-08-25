#!/bin/bash
# run_client.sh — run the ucx_perftest client against a server started with
# run_server.sh, and print the bandwidth result.
#
# Usage:
#   bash run_client.sh <client_host> <server_private_ip> <conda_env> [UCX_TLS_value]
#
# Examples:
#   bash run_client.sh <client_ip> <server_priv_ip> cudf-polars                 # SRD/EFA
#   bash run_client.sh <client_ip> <server_priv_ip> cudf-polars tcp,cuda_copy,cuda_ipc,sm,self   # TCP only
#
# UCX_TCP_TUNED=1: apply a tuned plain-TCP UCX config (see common.sh) for
# a fair comparison against TCP's out-of-the-box defaults:
#   UCX_TCP_TUNED=1 bash run_client.sh <client_ip> <server_priv_ip> cudf-polars tcp,cuda_copy,cuda_ipc,sm,self
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HOST="${1:?client host}"
SERVER_PRIV="${2:?server private ip}"
ENV_NAME="${3:?conda env name}"
TLS="${4:-}"

BIN="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ucx_perftest"

TLS_PREFIX=""
[[ -n "$TLS" ]] && TLS_PREFIX="UCX_TLS=${TLS} "

TCP_TUNING_PREFIX=""
if [[ "${UCX_TCP_TUNED:-0}" == "1" ]]; then
    echo "==> [client:$HOST] raising net.core.rmem_max/wmem_max and applying tuned TCP UCX settings"
    ensure_tcp_sysctl "$HOST"
    TCP_TUNING_PREFIX="UCX_TCP_TX_SEG_SIZE=256K UCX_TCP_RX_SEG_SIZE=256K UCX_TCP_MAX_BW=auto UCX_TCP_SNDBUF=4M UCX_TCP_RCVBUF=4M "
fi

echo "==> [client:$HOST] connecting to $SERVER_PRIV, UCX_TLS=${TLS:-<unset, auto-selects srd/EFA>} UCX_TCP_TUNED=${UCX_TCP_TUNED:-0}"
ssh_run "$HOST" \
  "UCX_LOG_LEVEL=info ${TLS_PREFIX}${TCP_TUNING_PREFIX}${BIN} -m cuda -t tag_bw -n 10 -s \$((1024*1024*100)) ${SERVER_PRIV}"
