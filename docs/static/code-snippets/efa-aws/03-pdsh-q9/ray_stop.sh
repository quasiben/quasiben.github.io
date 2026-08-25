#!/bin/bash
# ray_stop.sh — stop ray on a node.
# Usage: bash ray_stop.sh <pub_ip> <conda_env>
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

HOST="${1:?host}"
ENV_NAME="${2:?conda env name}"
RAY="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}/bin/ray"
# Graceful stop first (SIGTERM, lets workers cleanly deregister GPU/EFA
# memory) — --force (SIGKILL) leaves orphaned memory registrations on the
# EFA device, visible in dmesg as repeated
# "Failed to process command DEREG_MR ... err -22" bursts that accumulate
# across runs and degrade subsequent benchmark performance. Only fall back
# to --force if the graceful stop doesn't finish in time.
ssh_run "$HOST" "$RAY stop --grace-period 30" || ssh_run "$HOST" "$RAY stop --force"
echo "==> [$HOST] ray stopped"
