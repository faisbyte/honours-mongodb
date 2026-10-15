#!/bin/bash
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate

restore() {
  echo "restoring writeConcernMajorityJournalDefault=true"
  mongosh --port 27017 --quiet --eval 'c = rs.conf(); c.writeConcernMajorityJournalDefault = true; printjson(rs.reconfig(c).ok)'
}
trap restore EXIT

set_jd() {
  mongosh --port 27017 --quiet --eval "c = rs.conf(); c.writeConcernMajorityJournalDefault = $1; printjson(rs.reconfig(c))"
  for i in $(seq 1 12); do
    [ "$(mongosh --port 27017 --quiet --eval 'rs.conf().writeConcernMajorityJournalDefault')" = "$1" ] && { sleep 10; return 0; }
    sleep 5
  done
  echo "setting never reached $1"; return 1
}

for round in 1 2; do
  if [ $round = 1 ]; then JDS="true false"; JS="jnone jfalse jtrue"
  else JDS="false true"; JS="jtrue jfalse jnone"; fi
  for jd in $JDS; do
    set_jd $jd || exit 1
    for j in $JS; do
      echo "=== round $round  journalDefault=$jd  $j ==="
      python -m harness.run_all --config config/a2-$j-jd$jd.yaml --targets rs-primary-local --no-calibrate
    done
  done
done
