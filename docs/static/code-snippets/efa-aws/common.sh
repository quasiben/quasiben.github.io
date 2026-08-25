# common.sh — shared SSH/env helpers for the scripts in this directory.
# Source this, don't run it directly.
#
# Configure via env vars before sourcing:
#   SSH_KEY_PATH   path to the private key used to reach the nodes
#                  (default: ~/.ssh/id_ed25519)
#   REMOTE_USER    remote username on the nodes (default: ubuntu)
#   AWS_REGION_    AWS region the instances live in (default: us-east-2)

SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/id_ed25519}"
REMOTE_USER="${REMOTE_USER:-ubuntu}"
AWS_REGION_="${AWS_REGION_:-us-east-2}"

SSH_OPTS=(-i "$SSH_KEY_PATH" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)

ssh_run() {
    # ssh_run <host> <remote command string>
    local host="$1"; shift
    ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" "$@"
}

scp_get() {
    # scp_get <host> <remote path> <local path>
    local host="$1" remote="$2" local="$3"
    scp "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}:${remote}" "$local"
}

detect_env() {
    # detect_env <host> -> prints the first matching conda env name that has
    # cudf_polars + ray installed. Env naming isn't guaranteed consistent
    # across your own launch scripts, so this checks a couple of common
    # names — adjust the list to match your setup.
    local host="$1"
    ssh_run "$host" '
        for e in cuda-polars cudf-polars; do
            if [ -x "$HOME/miniforge3/envs/$e/bin/python" ] && \
               "$HOME/miniforge3/envs/$e/bin/python" -c "import cudf_polars, ray" 2>/dev/null; then
                echo "$e"
                exit 0
            fi
        done
        echo "NOTFOUND"
        exit 1
    '
}

private_ip() {
    local host="$1"
    ssh_run "$host" "hostname -I | awk '{print \$1}'"
}

gpu_count() {
    local host="$1"
    ssh_run "$host" "nvidia-smi --query-gpu=count --format=csv,noheader | head -1"
}

ensure_efa_peermem() {
    # ensure_efa_peermem <host> — force-load efa_nv_peermem before any CUDA
    # memory registration happens on this node.
    #
    # On a fresh boot (or after the module gets unloaded), the module is
    # not loaded yet. UCX checks /sys/module/efa_nv_peermem/version once at
    # UCP context init — if absent, it *silently and permanently* disables
    # GPUDirect RDMA for that process's lifetime and falls back to slow
    # host-staged cuda_copy, with no error. Run this before starting any
    # long-lived process (e.g. `ray start`) that will touch CUDA memory
    # registration, on every node, every boot.
    local host="$1"
    ssh_run "$host" "sudo modprobe efa_nv_peermem && cat /sys/module/efa_nv_peermem/version"
}

ensure_tcp_sysctl() {
    # ensure_tcp_sysctl <host> — raise net.core.rmem_max/wmem_max to 128MB.
    # Default on fresh Ubuntu AMIs is ~208KB, which silently clamps any
    # UCX_TCP_SNDBUF/RCVBUF request above that ceiling — tuning UCX's TCP
    # buffer sizes has no effect until this is raised too. Not persisted
    # across reboot.
    local host="$1"
    ssh_run "$host" "sudo sysctl -w net.core.rmem_max=134217728 >/dev/null && sudo sysctl -w net.core.wmem_max=134217728 >/dev/null"
}

# Shell snippet exported alongside UCX_TLS to build a tuned plain-TCP UCX
# config, for a fair SRD-vs-TCP comparison instead of TCP's out-of-the-box
# defaults. Values confirmed via a `ucx_perftest -m cuda -t tag_bw` sweep:
# default 8K TX segment size is the single biggest lever, MAX_BW=auto
# removes UCX's own 2200MBps cap, and SNDBUF/RCVBUF=4M only takes effect
# once ensure_tcp_sysctl has raised the OS ceiling above it. Pair with
# UCX_TLS=tcp,cuda_copy,cuda_ipc,sm,self to force plain TCP.
UCX_TCP_TUNING_SNIPPET='
export UCX_TCP_TX_SEG_SIZE=256K
export UCX_TCP_RX_SEG_SIZE=256K
export UCX_TCP_MAX_BW=auto
export UCX_TCP_SNDBUF=4M
export UCX_TCP_RCVBUF=4M
'

# Shell snippet (to be embedded in a remote command string) that fetches
# this node's IAM-role credentials via IMDSv2 and exports them as AWS_*
# env vars. Some S3 client libraries (e.g. kvikio) don't pick up
# credentials via the EC2 instance-profile chain on their own. Long-lived
# worker processes (e.g. Ray workers) inherit env from whatever shell
# started them, not from a separate driver process — so this needs to run
# on every node before starting those processes, not just once on a head
# node. Requires jq to be installed on the remote node.
AWS_CREDS_SNIPPET='
IMDS_TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
ROLE=$(curl -fsS -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/iam/security-credentials/)
CREDS=$(curl -fsS -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE)
export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | jq -r .AccessKeyId)
export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | jq -r .SecretAccessKey)
export AWS_SESSION_TOKEN=$(echo "$CREDS" | jq -r .Token)
export AWS_DEFAULT_REGION="'"${AWS_REGION_}"'"
export AWS_REGION="'"${AWS_REGION_}"'"
'

# cudf-polars / rapidsmpf / kvikio tuning env vars. Like AWS_CREDS_SNIPPET,
# these need to be set on every node before starting long-lived worker
# processes, not just in a benchmark driver's shell — the actual work
# happens in worker processes that inherit env from wherever they were
# started, not from a separate driver.
#
# Field names verified against the installed cudf_polars.utils.config
# dataclasses (ParquetOptions, StreamingExecutor) rather than guessed from
# --help text, which only documents flags that have a dedicated CLI option.
CUDF_POLARS_ENV_SNIPPET='
export CUDF_POLARS__PARQUET_OPTIONS__PREFETCH_FILE_METADATA=1
export CUDF_POLARS__PARQUET_OPTIONS__MAX_FOOTER_SAMPLES=3
export CUDF_POLARS__PARQUET_OPTIONS__MAX_ROW_GROUP_SAMPLES=1
export CUDF_POLARS__PARQUET_OPTIONS__PASS_READ_LIMIT=0

export CUDF_POLARS__EXECUTOR__TARGET_PARTITION_SIZE=2500000000
export CUDF_POLARS__EXECUTOR__MAX_CONCURRENT_IO_TASKS=16
export CUDF_POLARS__EXECUTOR__MAX_ROWS_PER_PARTITION=1000000
export CUDF_POLARS__EXECUTOR__BROADCAST_LIMIT=15396293836
export CUDF_POLARS__EXECUTOR__NUM_PY_EXECUTORS=8
export CUDF_POLARS__EXECUTOR__FALLBACK_MODE=warn
export CUDF_POLARS__EXECUTOR__MIN_DEVICE_SIZE=102641958912
export CUDF_POLARS__EXECUTOR__KVIKIO_NTHREADS=256

export RAPIDSMPF_NUM_STREAMING_THREADS=4
export RAPIDSMPF_UCXX_PROGRESS_MODE=thread-polling

export KVIKIO_NTHREADS=256
export KVIKIO_TASK_SIZE=67108864

export POLARS_MAX_THREADS=1
export OMP_NUM_THREADS=1
'
