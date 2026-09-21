#!/usr/bin/env bash
# Reproduces the yabas blog's "Scaled Spilling" study at 8-rank (NVL8) scale on a
# single DGX B200 node. Mirrors the log format used in ../blog-outputs-max-perf.
set -uo pipefail

CONDA_ENV="2026-09-15-cudf-polars"
OUT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RANKS=8
OUT_PARTS=8
COLUMNS=10
ROWS=536870912
LOCAL_INPUT_GIB=20
WARMUPS=3
RUNS=10

set +u
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${CONDA_ENV}"
set -u

# label:device_limit_mib  (device_limit_mib empty => no -l flag / unlimited)
EXPERIMENTS=(
  "no-spill:"
  "onset-spill:32768"
  "light-spill:28672"
  "moderate-spill:24576"
  "heavy-spill:20480"
  "very-heavy-spill:16384"
  "extreme-spill:12288"
)

total=${#EXPERIMENTS[@]}
i=0
for entry in "${EXPERIMENTS[@]}"; do
  i=$((i + 1))
  label="${entry%%:*}"
  limit_mib="${entry#*:}"

  extra_args=()
  if [[ -n "${limit_mib}" ]]; then
    extra_args=(-l "${limit_mib}")
  fi

  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  experiment="single-dgxb200-spill-study-${label}-${ts}"
  logfile="${OUT_DIR}/${experiment}.log"

  cmd=(rrun -n "${RANKS}" --bind-to cpu --bind-to memory
       -x UCX_MAX_RNDV_RAILS=1 -x UCX_PROTO_ENABLE=y -x UCX_WARN_UNUSED_ENV_VARS=n
       libcudf_streaming_bench_shuffle -C ucxx -w "${WARMUPS}" -r "${RUNS}" -m pool
       -g -s -x -p 1 -o "${OUT_PARTS}" -c "${COLUMNS}" -n "${ROWS}" "${extra_args[@]}")

  {
    echo "============================================================"
    echo "experiment=${experiment} (${i}/${total})"
    echo "start_utc=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
    echo "hostname=$(hostname)"
    echo "spill_label=${label}"
    echo "device_limit_mib=${limit_mib:-unlimited}"
    echo "local_input_gib=${LOCAL_INPUT_GIB}"
    echo "ranks=${RANKS}"
    echo "warmups=${WARMUPS} runs=${RUNS}"
    echo "command= ${cmd[*]}"
    echo "============================================================"
    echo
  } | tee "${logfile}"

  "${cmd[@]}" >>"${logfile}" 2>&1
  exit_code=$?

  {
    echo "end_utc=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
    echo "exit_code=${exit_code}"
  } >>"${logfile}"

  echo "[${label}] exit_code=${exit_code} -> ${logfile}"

  if [[ "${exit_code}" -ne 0 ]]; then
    echo "Run '${label}' failed with exit_code=${exit_code}; stopping sweep." >&2
    exit "${exit_code}"
  fi
done
