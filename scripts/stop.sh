#!/bin/bash
# Stop the local four-node replica set rs0: secondaries first, then the primary.
#
# Usage: scripts/stop.sh
#
# The primary goes last and with force: true. By then no secondary is left to
# catch up, and without force the primary refuses to shut down for that
# reason. Nodes that are already down are skipped.
set -uo pipefail

PORTS="27017 27018 27019 27020"

up() { mongosh --port "$1" --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; }

# The shutdown command drops the connection, so mongosh reports an error even
# when it worked. Success is judged by the port going quiet afterwards.
shutdown_node() {
  mongosh --port "$1" --quiet --eval "db.adminCommand({shutdown: 1$2})" >/dev/null 2>&1 || true
}

primary=""
for port in $PORTS; do
  up "$port" || { echo "port $port: not running"; continue; }
  if [ "$(mongosh --port "$port" --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null)" = "true" ]; then
    primary=$port
  else
    echo "port $port: shutting down secondary"
    shutdown_node "$port" ""
  fi
done

if [ -n "$primary" ]; then
  echo "port $primary: shutting down primary"
  shutdown_node "$primary" ", force: true"
fi

for _ in $(seq 1 30); do
  running=""
  for port in $PORTS; do up "$port" && running="$running $port"; done
  [ -z "$running" ] && { echo "all nodes stopped"; exit 0; }
  sleep 1
done
echo "still running after 30 s:$running" >&2
exit 1
