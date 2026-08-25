#!/bin/bash
# ray_status.sh — print `ray status` from a node in the cluster.
# Usage: bash ray_status.sh <pub_ip> <conda_env>
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HOST="${1:?host}"
ENV_NAME="${2:?conda env name}"
RAY="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ray"
ssh_run "$HOST" "$RAY status"
