#!/bin/bash
# Start the local four-node replica set rs0 and set chainingAllowed explicitly.
#
# Usage: scripts/start.sh chained|unchained
#
# chainingAllowed is stored in the replica set config and survives restarts,
# so without this a run inherits whatever the last experiment left. The
# argument is required and the setting is always applied and checked.
#
# This sets the permission, not the layout: with chaining allowed MongoDB may
# still have every secondary sync from the primary (scripts/a1_alternate.sh
# uses replSetSyncFrom to force a chain). Each config's expect block checks
# the actual layout before a run starts.
#
# Same mongod flags as the original ~/mongo-rs/start.sh. Data lives in
# $MONGO_RS_DIR (default ~/mongo-rs), one dbpath per node, and the set must
# already have been initiated once; this script does not run rs.initiate().
set -euo pipefail

case "${1:-}" in
  chained)   CHAINING=true ;;
  unchained) CHAINING=false ;;
  *) echo "usage: $0 chained|unchained" >&2; exit 2 ;;
esac

RS_DIR="${MONGO_RS_DIR:-$HOME/mongo-rs}"
PRIMARY_PORT=27017   # priority 10 in the replica set config

up() { mongosh --port "$1" --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; }

for i in 0 1 2 3; do
  port=$((27017 + i))
  if up "$port"; then
    echo "port $port: mongod already running, left as is"
    continue
  fi
  echo "port $port: starting mongod on $RS_DIR/n$i"
  nohup mongod --replSet rs0 --port "$port" --dbpath "$RS_DIR/n$i" \
    --bind_ip 127.0.0.1 --logpath "$RS_DIR/n$i/mongod.log" > /dev/null 2>&1 &
done

echo "waiting for $PRIMARY_PORT to become primary"
for _ in $(seq 1 60); do
  if [ "$(mongosh --port $PRIMARY_PORT --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null)" = "true" ]; then
    break
  fi
  sleep 1
done
if [ "$(mongosh --port $PRIMARY_PORT --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null)" != "true" ]; then
  echo "$PRIMARY_PORT did not become primary within 60 s" >&2
  exit 1
fi

current=$(mongosh --port $PRIMARY_PORT --quiet --eval 'rs.conf().settings.chainingAllowed')
if [ "$current" != "$CHAINING" ]; then
  echo "setting chainingAllowed=$CHAINING (was $current)"
  mongosh --port $PRIMARY_PORT --quiet --eval \
    "c = rs.conf(); c.settings.chainingAllowed = $CHAINING; rs.reconfig(c).ok" > /dev/null
fi
current=$(mongosh --port $PRIMARY_PORT --quiet --eval 'rs.conf().settings.chainingAllowed')
if [ "$current" != "$CHAINING" ]; then
  echo "chainingAllowed is $current after reconfig, expected $CHAINING" >&2
  exit 1
fi
echo "chainingAllowed=$CHAINING"

# Sync sources can take a few heartbeats to settle after a start.
mongosh --port $PRIMARY_PORT --quiet --eval \
  'rs.status().members.forEach(m => print(m.name, m.stateStr, "syncs from", m.syncSourceHost || "-"))'
