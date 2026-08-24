#!/usr/bin/env bash
# launch_efa_nodes.sh — launch N EFA-enabled EC2 instances, install CUDA +
# EFA + miniforge, and build a cudf-polars + rapidsmpf conda environment on
# each. GPUDirect RDMA over EFA requires a multi-GPU instance size (e.g.
# g7e.12xlarge or larger) — see the main post for why.
#
# Usage:
#   bash launch_efa_nodes.sh
#   bash launch_efa_nodes.sh --terminate i-0abc123 i-0def456
#
# Configure via env vars (all have defaults except the ones marked
# required — you must set those, or edit the defaults below, before
# running):
#   AWS_REGION_        default: us-east-2 (the same variable common.sh reads,
#                       so one export covers every script here; AWS_REGION is
#                       also accepted)
#   INSTANCE_TYPE      default: g7e.12xlarge
#   AMI_ID             default: ami-07062e2a343acc423 (Ubuntu 24.04, us-east-2 — check the
#                       AWS console for the current AMI ID in your region)
#   IAM_PROFILE         required — an instance profile with S3 read access to
#                       your dataset bucket
#   SECURITY_GROUP      required — must allow SSH from your IP and all
#                       traffic between members of the group itself (EFA/UCX
#                       needs inter-node traffic open)
#   SUBNET_ID           required
#   KEY_NAME            required — an EC2 key pair name already in your account
#   SSH_KEY_PATH        default: ~/.ssh/<KEY_NAME>.pem — common.sh defaults to
#                       ~/.ssh/id_ed25519 instead, so export this explicitly if
#                       you want the benchmark scripts to reuse the same key
#   AWS_PROFILE_        default: default (the AWS CLI profile to use)
#   COUNT               default: 2
#   NAME_TAG            default: efa-bench
set -euo pipefail

REGION="${AWS_REGION_:-${AWS_REGION:-us-east-2}}"
INSTANCE_TYPE="${INSTANCE_TYPE:-g7e.12xlarge}"
AMI_ID="${AMI_ID:-ami-07062e2a343acc423}"
COUNT="${COUNT:-2}"
NAME_TAG="${NAME_TAG:-efa-bench}"
TERMINATE_MODE=false
TERMINATE_IDS=()

# Arguments are parsed before the launch-only requirements are validated, so
# teardown doesn't need the full launch configuration exported.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --count) COUNT="$2"; shift 2 ;;
        --name)  NAME_TAG="$2"; shift 2 ;;
        --terminate|--stop)
            if [[ "$1" == "--stop" ]]; then
                echo "WARNING: --stop is deprecated, use --terminate. This destroys" >&2
                echo "         the instances and their root volumes." >&2
            fi
            TERMINATE_MODE=true; shift
            while [[ $# -gt 0 && "$1" != --* ]]; do
                TERMINATE_IDS+=("$1"); shift
            done
            ;;
        *) echo "Unknown: $1" >&2; exit 1 ;;
    esac
done

export AWS_PROFILE="${AWS_PROFILE_:-default}"

# ---------------------------------------------------------------------------
# Terminate mode — instances are launched with DeleteOnTermination=true, so
# this destroys the root volumes along with them.
# ---------------------------------------------------------------------------
if $TERMINATE_MODE; then
    if [[ ${#TERMINATE_IDS[@]} -eq 0 ]]; then
        echo "--terminate needs at least one instance id" >&2
        exit 1
    fi
    aws ec2 terminate-instances \
        --region "$REGION" \
        --instance-ids "${TERMINATE_IDS[@]}" \
        --query 'TerminatingInstances[].{ID:InstanceId,State:CurrentState.Name}' \
        --output table
    exit 0
fi

IAM_PROFILE="${IAM_PROFILE:?set IAM_PROFILE to an instance profile with S3 read access}"
SECURITY_GROUP="${SECURITY_GROUP:?set SECURITY_GROUP to a security group id (sg-...)}"
SUBNET_ID="${SUBNET_ID:?set SUBNET_ID to a subnet id (subnet-...)}"
KEY_NAME="${KEY_NAME:?set KEY_NAME to an EC2 key pair name in your account}"

SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${KEY_NAME}.pem}"
SSH_OPTS="-i ${SSH_KEY_PATH} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ConnectTimeout=30"

# ---------------------------------------------------------------------------
# Launch instances
# ---------------------------------------------------------------------------
echo "==> Launching ${COUNT}x ${INSTANCE_TYPE} in ${REGION}..."

INSTANCE_IDS=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --count "$COUNT" \
    --key-name "$KEY_NAME" \
    --iam-instance-profile "Name=${IAM_PROFILE}" \
    --network-interfaces \
        "DeviceIndex=0,NetworkCardIndex=0,InterfaceType=efa,SubnetId=${SUBNET_ID},Groups=${SECURITY_GROUP},DeleteOnTermination=true" \
    --block-device-mappings \
        "DeviceName=/dev/sda1,Ebs={VolumeSize=500,VolumeType=gp3,DeleteOnTermination=true}" \
    --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=${NAME_TAG}}]" \
    --query 'Instances[].InstanceId' \
    --output text)

echo "==> Launched: ${INSTANCE_IDS}"
echo "==> Waiting for running state..."
# shellcheck disable=SC2086
aws ec2 wait instance-running --region "$REGION" --instance-ids $INSTANCE_IDS

# Collect public IPs
declare -a PUB_IPS
for id in $INSTANCE_IDS; do
    read -r PRIV_IP PUB_IP < <(aws ec2 describe-instances \
        --region "$REGION" --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].[PrivateIpAddress,PublicIpAddress]' \
        --output text)
    PUB_IPS+=("$PUB_IP")
    echo "  $id  private=$PRIV_IP  public=$PUB_IP"
done

# ---------------------------------------------------------------------------
# Wait for SSH to be ready
# ---------------------------------------------------------------------------
echo ""
echo "==> Waiting for SSH to come up on all nodes..."
for ip in "${PUB_IPS[@]}"; do
    echo -n "    $ip ..."
    until ssh $SSH_OPTS ubuntu@"$ip" true 2>/dev/null; do
        echo -n "."
        sleep 10
    done
    echo " ready"
done

# ---------------------------------------------------------------------------
# Setup script to run on each node
# ---------------------------------------------------------------------------
SETUP_SCRIPT=$(cat <<'SETUP'
#!/bin/bash
set -euo pipefail

echo "--- Waiting for cloud-init and unattended-upgrades to finish ---"
sudo cloud-init status --wait 2>/dev/null || true
sudo systemctl stop unattended-upgrades.service 2>/dev/null || true
sudo systemctl disable unattended-upgrades.service 2>/dev/null || true
sudo kill -9 $(pgrep -f unattended-upgrades) 2>/dev/null || true
sudo rm -f /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend /var/cache/apt/archives/lock
sudo dpkg --configure -a 2>/dev/null || true

echo "--- Adding NVIDIA CUDA repository ---"
wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
rm cuda-keyring_1.1-1_all.deb

echo "--- Installing CUDA toolkit (latest) and jq ---"
# jq parses the IMDS credential response in common.sh's AWS_CREDS_SNIPPET.
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y cuda jq

echo "--- Installing Miniforge ---"
curl -fsSL -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash "Miniforge3-$(uname)-$(uname -m).sh" -b -p "$HOME/miniforge3"
rm "Miniforge3-$(uname)-$(uname -m).sh"
"$HOME/miniforge3/bin/conda" init bash

echo "--- Installing AWS EFA software stack ---"
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
tar -xzf aws-efa-installer-latest.tar.gz
cd aws-efa-installer
sudo ./efa_installer.sh -y --skip-limit-conf
cd ..
rm -rf aws-efa-installer aws-efa-installer-latest.tar.gz

echo "--- Enabling efa_nv_peermem kernel module (required for CUDA) ---"
sudo modprobe efa_nv_peermem

echo "--- Enabling persistent loading of efa_nv_peermem kernel module ---"
echo "efa_nv_peermem" | sudo tee /etc/modules-load.d/efa_nv_peermem.conf

# librapidsmpf-tests / libcudf-streaming-tests ship libcudf_streaming_bench_shuffle
# (used by run_shuffle_bench.sh) — not included in the base cudf-polars/rapidsmpf
# packages.
echo "--- Creating cudf-polars conda environment ---"
"$HOME/miniforge3/bin/conda" create -y -n cudf-polars \
    -c rapidsai-nightly -c conda-forge \
    cudf "cudf-polars=26.10" ray-data \
    librapidsmpf-tests libcudf-streaming-tests

echo "--- Cloning rapidsmpf ---"
git clone https://github.com/rapidsai/rapidsmpf.git "$HOME/rapidsmpf"

echo "--- Creating rapidsmpf conda environment ---"
"$HOME/miniforge3/bin/conda" env create \
    -f "$HOME/rapidsmpf/conda/environments/all_cuda-133_arch-x86_64.yaml" \
    -n rapidsmpf

echo "--- Building rapidsmpf ---"
cd "$HOME/rapidsmpf"
conda run -n rapidsmpf ./build.sh
cd "$HOME"

echo "--- Done. Reboot to load NVIDIA kernel modules. ---"
SETUP
)

# ---------------------------------------------------------------------------
# Run setup on all nodes in parallel
# ---------------------------------------------------------------------------
echo ""
echo "==> Setting up all nodes in parallel (logs: /tmp/setup_<ip>.log)..."
for ip in "${PUB_IPS[@]}"; do
    ssh $SSH_OPTS ubuntu@"$ip" "bash -s" <<< "$SETUP_SCRIPT" 2>&1 | tee "/tmp/setup_${ip}.log" &
done
wait
echo "==> All nodes setup complete."

# ---------------------------------------------------------------------------
# Reboot and wait
# ---------------------------------------------------------------------------
echo ""
echo "==> Rebooting nodes to load NVIDIA kernel modules..."
for ip in "${PUB_IPS[@]}"; do
    ssh $SSH_OPTS ubuntu@"$ip" "sudo reboot" || true
done

echo "==> Waiting 60s for nodes to come back up..."
sleep 60

for ip in "${PUB_IPS[@]}"; do
    echo -n "    $ip ..."
    until ssh $SSH_OPTS ubuntu@"$ip" true 2>/dev/null; do
        echo -n "."
        sleep 10
    done
    echo " ready"
done

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
echo ""
echo "==> Verifying nvidia-smi on all nodes..."
for ip in "${PUB_IPS[@]}"; do
    echo "  --- $ip ---"
    ssh $SSH_OPTS ubuntu@"$ip" "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader"
done

echo ""
echo "================================================================================"
echo "Nodes ready. SSH:"
for ip in "${PUB_IPS[@]}"; do
    echo "  ssh -i ${SSH_KEY_PATH} -o IdentitiesOnly=yes ubuntu@${ip}"
done
echo ""
echo "Activate env on each node:"
echo "  conda activate cudf-polars"
echo ""
echo "Export these before running the benchmark scripts, so they reach the"
echo "same nodes with the same key:"
echo "  export SSH_KEY_PATH=${SSH_KEY_PATH}"
echo "  export AWS_REGION_=${REGION}"
echo ""
echo "Terminate when done (this also deletes the root volumes):"
echo "  bash $0 --terminate ${INSTANCE_IDS}"
echo "================================================================================"
