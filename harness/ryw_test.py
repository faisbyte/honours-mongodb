#!/usr/bin/env python3
"""Direct read-your-writes test for causal consistency.

The freshness curves cannot answer whether causal sessions are working:
  * with w:"majority" every read path is already 1.0 fresh, so there is no
    room above it for causality to improve on;
  * with w:1 and readConcern "majority" the docs say causal sessions do not
    promise read-your-writes anyway.

So test it directly. Each trial writes a value, then immediately reads it back
from a SECONDARY. Read-your-writes either holds or it does not.

Two arms, differing only in whether the reading session has been advanced with
the writer's operationTime and clusterTime:

    causal   - session advanced before the read
    plain    - no session at all

The discriminating signal is LATENCY, not just freshness. A working causal
read makes the server wait until the commit point passes the write, so it
should be slow and fresh. A plain read returns whatever the secondary has, so
it should be fast and sometimes stale. If both arms come back fast, the
session is not propagating.

--use-find-one tests a specific hypothesis for why the session might not be
propagating: pymongo injects afterClusterTime automatically when it builds a
command itself, but this script's default read path constructs a raw find
command with an explicit readConcern key already set, and that hand-built
command may pass through unmodified rather than getting afterClusterTime
merged in. Comparing the raw-command result against the find_one() result
isolates whether that is the cause.

Usage:
    python -m harness.ryw_test
    python -m harness.ryw_test --trials 2000 --write-concern 1
    python -m harness.ryw_test --write-concern majority --read-concern majority --use-find-one
"""

from __future__ import annotations

import argparse
import statistics
import time

from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import Secondary

HOSTS = "127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019,127.0.0.1:27020"
URI = f"mongodb://{HOSTS}/?replicaSet=rs0"
SECONDARY = Secondary()


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(int(len(xs) * p), len(xs) - 1)] if xs else float("nan")


def run_arm(client, arm: str, trials: int, wc, rc: str,
           use_find_one: bool = False) -> dict:
    """One arm. Returns freshness and latency for the read-after-write."""
    db = client.freshness
    coll = "probe_ryw"
    key = "ryw-1"

    session = client.start_session(causal_consistency=True) if arm == "causal" else None

    read_cmd = {"find": coll, "filter": {"_id": key}, "limit": 1,
                "singleBatch": True, "readConcern": {"level": rc}}

    fresh, stale, errors = 0, 0, 0
    latencies = []

    for seq in range(trials):
        try:
            reply = db.command({
                "update": coll,
                "updates": [{"q": {"_id": key}, "u": {"$set": {"seq": seq}},
                             "upsert": True}],
                "writeConcern": wc,
            }, session=session)
        except Exception:  # noqa: BLE001
            errors += 1
            continue

        if session is not None:
            op_time = reply.get("operationTime")
            cluster_time = reply.get("$clusterTime")
            if cluster_time is not None:
                session.advance_cluster_time(cluster_time)
            if op_time is not None:
                session.advance_operation_time(op_time)

        t0 = time.perf_counter_ns()
        try:
            if use_find_one:
                coll_obj = db.get_collection(
                    coll, read_preference=SECONDARY,
                    read_concern=ReadConcern(rc))
                doc = coll_obj.find_one({"_id": key}, session=session)
                t1 = time.perf_counter_ns()
                observed = doc.get("seq") if doc else None
            else:
                r = db.command(read_cmd, read_preference=SECONDARY,
                               session=session)
                t1 = time.perf_counter_ns()
                batch = r.get("cursor", {}).get("firstBatch", [])
                observed = batch[0].get("seq") if batch else None
        except Exception:  # noqa: BLE001
            errors += 1
            continue

        latencies.append(t1 - t0)
        if observed == seq:
            fresh += 1
        else:
            stale += 1

    if session is not None:
        session.end_session()

    n = fresh + stale
    return {
        "arm": arm,
        "trials": n,
        "errors": errors,
        "ryw_holds": fresh,
        "ryw_violated": stale,
        "ryw_rate": fresh / n if n else float("nan"),
        "p50_us": pct(latencies, 0.50) / 1000 if latencies else float("nan"),
        "p95_us": pct(latencies, 0.95) / 1000 if latencies else float("nan"),
        "max_us": max(latencies) / 1000 if latencies else float("nan"),
        "mean_us": statistics.fmean(latencies) / 1000 if latencies else float("nan"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=1000)
    ap.add_argument("--write-concern", default="1",
                    help='"1" or "majority"')
    ap.add_argument("--read-concern", default="majority",
                    help='"local" or "majority"')
    ap.add_argument("--use-find-one", action="store_true",
                    help="read via find_one() instead of a raw db.command, to "
                         "test whether afterClusterTime reaches the server")
    args = ap.parse_args()

    wc = {"w": int(args.write_concern)} if args.write_concern.isdigit() \
         else {"w": args.write_concern}

    client = MongoClient(URI, serverSelectionTimeoutMS=5000)
    print(f"write concern {wc}, read concern {args.read_concern!r}, "
          f"{args.trials} trials per arm, reads go to a SECONDARY, "
          f"read path {'find_one()' if args.use_find_one else 'raw db.command'}\n")

    results = [run_arm(client, arm, args.trials, wc, args.read_concern,
                       args.use_find_one)
              for arm in ("plain", "causal")]

    hdr = f"{'arm':8} {'trials':>7} {'RYW held':>9} {'rate':>8} " \
          f"{'p50 us':>9} {'p95 us':>9} {'max us':>10} {'errors':>7}"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        print(f"{r['arm']:8} {r['trials']:>7} {r['ryw_holds']:>9} "
              f"{r['ryw_rate']:>7.3%} {r['p50_us']:>9.1f} {r['p95_us']:>9.1f} "
              f"{r['max_us']:>10.1f} {r['errors']:>7}")

    plain, causal = results[0], results[1]
    print("\nreading:")
    if causal["ryw_rate"] > plain["ryw_rate"] + 0.05:
        print("  causal arm is fresher than plain. The session is doing something.")
    else:
        print("  causal and plain are equally fresh. No freshness signal.")
    if causal["p50_us"] > plain["p50_us"] * 2:
        print("  causal reads are markedly slower. The server is waiting for "
              "the commit point, which is what a working causal read does.")
    else:
        print("  causal reads are NOT slower than plain. If the session were "
              "propagating, the server would be waiting. It is not.")


if __name__ == "__main__":
    main()