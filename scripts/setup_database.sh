#!/usr/bin/env bash
set -ue

# The first argument is the hostname of the FDB container.
FDB_HOSTNAME=${1:-fdb}
echo "FDB_HOSTNAME=$FDB_HOSTNAME"

# The second argument is the description & ID of the FDB cluster.
FDB_DESCRIPTION_ID=${2:-docker:docker}
echo "FDB_DESCRIPTION_ID=$FDB_DESCRIPTION_ID"

# Wait for DNS resolution of FDB hostname.
echo "Waiting for FDB hostname to resolve..."
MAX_ATTEMPTS=30
ATTEMPT=0
FDB_IP=""
while [[ -z "$FDB_IP" ]]; do
  FDB_IP=$(getent hosts "$FDB_HOSTNAME" | awk '{print $1}') || true
  if [[ -z "$FDB_IP" ]]; then
    ATTEMPT=$((ATTEMPT + 1))
    if [[ $ATTEMPT -ge $MAX_ATTEMPTS ]]; then
      echo "ERR! Failed to resolve $FDB_HOSTNAME after $MAX_ATTEMPTS attempts"
      exit 1
    fi
    echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for DNS..."
    sleep 1
  fi
done
echo "FDB_IP=$FDB_IP"

export FDB_CLUSTER_FILE="/etc/foundationdb/fdb.cluster"

# Create the FDB cluster file.
echo "${FDB_DESCRIPTION_ID}@${FDB_IP}:4500" > "$FDB_CLUSTER_FILE"
echo "FDB_CLUSTER_FILE: $(cat "$FDB_CLUSTER_FILE")"

# Wait for FDB to be ready.
echo "Waiting for FDB to be ready..."
ATTEMPT=0
while ! fdbcli --exec 'status' &>/dev/null; do
  ATTEMPT=$((ATTEMPT + 1))
  if [[ $ATTEMPT -ge $MAX_ATTEMPTS ]]; then
    echo "ERR! FDB not ready after $MAX_ATTEMPTS attempts"
    exit 1
  fi
  echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS - FDB not ready, waiting..."
  sleep 1
done
echo "FDB is ready."
