#!/bin/bash
# setup_mpi_ssh.sh — generate an ephemeral keypair on the launcher node
# (node A) and authorize it on both nodes, so mpirun on node A can SSH out
# to node B to launch remote MPI ranks. The private key is generated on
# and never leaves the launcher node.
#
# Usage: bash setup_mpi_ssh.sh <launcher_pub_ip> <other_pub_ip>
set -euo pipefail
cd "$(dirname "$0")/.."
source ./common.sh

LAUNCHER="${1:?launcher pub ip (node A, runs mpirun)}"
OTHER="${2:?other pub ip (node B)}"

echo "==> Generating ephemeral keypair on launcher ($LAUNCHER)"
ssh_run "$LAUNCHER" "
[ -f ~/.ssh/mpi_ephemeral ] || ssh-keygen -t ed25519 -N '' -f ~/.ssh/mpi_ephemeral -q
cat ~/.ssh/mpi_ephemeral.pub
"

PUBKEY=$(ssh_run "$LAUNCHER" "cat ~/.ssh/mpi_ephemeral.pub")

echo "==> Authorizing launcher's key on itself (localhost) and on $OTHER"
ssh_run "$LAUNCHER" "grep -qF \"\$(cat ~/.ssh/mpi_ephemeral.pub)\" ~/.ssh/authorized_keys 2>/dev/null || cat ~/.ssh/mpi_ephemeral.pub >> ~/.ssh/authorized_keys"
ssh_run "$OTHER" "grep -qF '${PUBKEY}' ~/.ssh/authorized_keys 2>/dev/null || echo '${PUBKEY}' >> ~/.ssh/authorized_keys"

echo "==> Testing SSH from launcher to itself and to $OTHER via private IPs"
LAUNCHER_PRIV=$(private_ip "$LAUNCHER")
OTHER_PRIV=$(private_ip "$OTHER")
ssh_run "$LAUNCHER" "ssh -i ~/.ssh/mpi_ephemeral -o StrictHostKeyChecking=accept-new -o BatchMode=yes ${REMOTE_USER}@${LAUNCHER_PRIV} hostname"
ssh_run "$LAUNCHER" "ssh -i ~/.ssh/mpi_ephemeral -o StrictHostKeyChecking=accept-new -o BatchMode=yes ${REMOTE_USER}@${OTHER_PRIV} hostname"
echo "==> SSH setup OK. launcher_priv=${LAUNCHER_PRIV} other_priv=${OTHER_PRIV}"
