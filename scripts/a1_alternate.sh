#!/bin/bash
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate

TARGETS="direct-27017 direct-27018 direct-27019 direct-27020"
CHAINED='[["27017","-"],["27018","27020"],["27019","27020"],["27020","27017"]]'
UNCHAINED='[["27017","-"],["27018","27017"],["27019","27017"],["27020","27017"]]'

topology() {
  mongosh --port 27017 --quiet --eval \
    'print(JSON.stringify(rs.status().members.map(m => [m.name.slice(-5), m.syncSourceHost ? m.syncSourceHost.slice(-5) : "-"])))'
}

wait_for() {
  for i in $(seq 1 18); do
    [ "$(topology)" = "$1" ] && return 0
    sleep 5
  done
  echo "topology never reached expected state: $(topology)"
  return 1
}

set_condition() {
  if [ "$1" = chained ]; then
    mongosh --port 27017 --quiet --eval 'c = rs.conf(); c.settings.chainingAllowed = true; rs.reconfig(c)' > /dev/null
    for p in 27018 27019; do
      mongosh --port $p --quiet --eval 'db.adminCommand({replSetSyncFrom: "127.0.0.1:27020"})' > /dev/null
    done
    wait_for "$CHAINED"
  else
    mongosh --port 27017 --quiet --eval 'c = rs.conf(); c.settings.chainingAllowed = false; rs.reconfig(c)' > /dev/null
    wait_for "$UNCHAINED"
  fi
}

for order in "unchained chained" "chained unchained" "unchained chained"; do
  for cond in $order; do
    echo "=== $cond ==="
    set_condition $cond || { echo "aborting"; exit 1; }
    python -m harness.run_all --config config/a1-$cond.yaml --targets $TARGETS --no-calibrate
  done
done
