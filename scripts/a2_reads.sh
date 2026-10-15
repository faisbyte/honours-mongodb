#!/bin/bash
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate

UNCHAINED='[["27017","-"],["27018","27017"],["27019","27017"],["27020","27017"]]'

topology() {
  mongosh --port 27017 --quiet --eval \
    'print(JSON.stringify(rs.status().members.map(m => [m.name.slice(-5), m.syncSourceHost ? m.syncSourceHost.slice(-5) : "-"])))'
}

check_topo() {
  if [ "$(topology)" != "$UNCHAINED" ]; then
    echo "topology is not the unchained layout:"; topology; return 1
  fi
}

restore() {
  echo "restoring writeConcernMajorityJournalDefault=true"
  mongosh --port 27017 --quiet --eval 'c = rs.conf(); c.writeConcernMajorityJournalDefault = true; printjson(rs.reconfig(c).ok)'
}
trap restore EXIT
trap 'exit 130' INT TERM

set_jd() {
  mongosh --port 27017 --quiet --eval "c = rs.conf(); c.writeConcernMajorityJournalDefault = $1; printjson(rs.reconfig(c).ok)"
  for i in $(seq 1 12); do
    [ "$(mongosh --port 27017 --quiet --eval 'rs.conf().writeConcernMajorityJournalDefault')" = "$1" ] && { sleep 10; return 0; }
    sleep 5
  done
  echo "setting never reached $1"; return 1
}

check_topo || exit 1

for round in 1 2; do
  if [ $round = 1 ]; then
    JDS="true false"
    TS="rs-secondary-majority rs-secondary-causal-majority rs-secondary-local"
  else
    JDS="false true"
    TS="rs-secondary-local rs-secondary-causal-majority rs-secondary-majority"
  fi
  for jd in $JDS; do
    set_jd $jd || exit 1
    check_topo || exit 1
    echo "=== round $round  journalDefault=$jd ==="
    python -m harness.run_all --config config/a2-reads-w1-jd$jd.yaml --targets $TS --no-calibrate
  done
done
