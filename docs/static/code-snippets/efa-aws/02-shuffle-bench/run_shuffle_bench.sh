#!/bin/bash
# run_shuffle_bench.sh — run libcudf_streaming_bench_shuffle across 2 nodes
# with the UCXX communicator, EFA path, launched via mpirun. GPU/CPU/NUMA
# binding per rank is handled by binder.sh
# (https://github.com/LStuber/binding), one rank per GPU, deployed to both
# nodes automatically.
#
# Usage:
#   bash setup_mpi_ssh.sh <node_a_pub> <node_b_pub>   # one-time per pair
#   bash run_shuffle_bench.sh <node_a_pub> <node_b_pub> <conda_env> [extra bench args...]
#
# UCX_TLS: export before calling to override transport, e.g. to exclude
# srd/EFA and force tcp:
#   UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self bash run_shuffle_bench.sh ...
# Left unset, UCX auto-selects srd/EFA for inter-node traffic as normal.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

NODE_A="${1:?node A public ip (mpirun launcher)}"
NODE_B="${2:?node B public ip}"
ENV_NAME="${3:?conda env name}"
shift 3
EXTRA_ARGS=("$@")

ENVROOT="/home/${REMOTE_USER}/miniforge3/envs/${ENV_NAME}"
BIN="${ENVROOT}/bin/libcudf_streaming_bench_shuffle"
MPIRUN="${ENVROOT}/bin/mpirun"

PRIV_A=$(private_ip "$NODE_A")
PRIV_B=$(private_ip "$NODE_B")
GPUS_A=$(gpu_count "$NODE_A")
GPUS_B=$(gpu_count "$NODE_B")
TOTAL_RANKS=$((GPUS_A + GPUS_B))
echo "==> node A (launcher): $NODE_A / $PRIV_A / ${GPUS_A} GPU(s)"
echo "==> node B:            $NODE_B / $PRIV_B / ${GPUS_B} GPU(s)"
echo "==> total ranks: ${TOTAL_RANKS} (1 rank per GPU)"

echo "==> ensuring efa_nv_peermem is loaded on both nodes"
ensure_efa_peermem "$NODE_A"
ensure_efa_peermem "$NODE_B"

echo "==> deploying binder.sh (https://github.com/LStuber/binding) to both nodes"
scp "${SSH_OPTS[@]}" ./02-shuffle-bench/binder.sh "${REMOTE_USER}@${NODE_A}:~/binder.sh"
scp "${SSH_OPTS[@]}" ./02-shuffle-bench/binder.sh "${REMOTE_USER}@${NODE_B}:~/binder.sh"
ssh_run "$NODE_A" "chmod +x ~/binder.sh"
ssh_run "$NODE_B" "chmod +x ~/binder.sh"

if [[ ${#EXTRA_ARGS[@]} -eq 0 ]]; then
    # 20 GiB/rank at 1 GiB per input partition, c=1 (default, 4 bytes/row):
    #   -n = 1024*1024*1024 / 4 = 268435456 rows -> exactly 1024 MiB/partition
    #   -p 20 partitions * 1 GiB = 20 GiB/rank
    #   -o 8: cudf columns cap at ~2^31 rows (int32 row counts). With
    #     TOTAL_RANKS ranks (1 per GPU), global rows = 20GiB * TOTAL_RANKS /
    #     4 bytes, hashed into o*TOTAL_RANKS output buckets. o=8 keeps
    #     rows/bucket in a safe range well under 2^31.
    #   -l omitted: unlimited (binary default is -1), full GPU available.
    EXTRA_ARGS=(-w 3 -r 10 -g -s -x -n 268435456 -p 20 -o 8)
fi

UCX_TLS_FLAG=""
if [[ -n "${UCX_TLS:-}" ]]; then
    UCX_TLS_FLAG="-x UCX_TLS=${UCX_TLS}"
fi
echo "==> bench args: -C ucxx ${EXTRA_ARGS[*]}"
echo "==> UCX_TLS: ${UCX_TLS:-<unset, UCX auto-selects>}"

ssh_run "$NODE_A" "
$MPIRUN --host ${PRIV_A}:${GPUS_A},${PRIV_B}:${GPUS_B} -np ${TOTAL_RANKS} \
    --mca plm_rsh_agent 'ssh -i /home/${REMOTE_USER}/.ssh/mpi_ephemeral -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes' \
    --prtemca plm_rsh_agent 'ssh -i /home/${REMOTE_USER}/.ssh/mpi_ephemeral -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes' \
    -x RAPIDSMPF_UCXX_PROGRESS_MODE=thread-polling \
    ${UCX_TLS_FLAG} \
    ~/binder.sh $BIN -C ucxx ${EXTRA_ARGS[*]}
"
