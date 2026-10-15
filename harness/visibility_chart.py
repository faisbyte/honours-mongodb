#!/usr/bin/env python3
"""Bar chart for the "Progress so far" slide, computed from collected runs.

Chart: how long after a write is sent until the new value can be read, for
three read paths. Source experiment: "when each read becomes visible per read
path", write concern w:1, replica set journal default ON.

Method. This mirrors visibility_for_run() in harness/event_timings.py, which
produced the existing visibility plot (analyse.py draws the freshness curves
and tests equality, event_timings.py gives the per-write visibility times):
  1. Attach each read to the most recently SENT write (read start >= write start).
  2. A read is fresh if observed_seq >= that write's seq.
  3. Per write, take the first fresh read. Visibility time = END of that read
     minus the time the write was sent (the same upper bound the plot used).
  4. Warmup writes, errored writes and writes that never get a fresh read are
     excluded (and counted in the printout).
Every run is processed on its own and nothing is mixed across runs, because
perf_counter_ns timestamps from different processes share no epoch.

Run from the repo root:
    python visibility_chart.py
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from harness import runs

# ---------------------------------------------------------------------------
# CONFIG: run selection and column names
# ---------------------------------------------------------------------------
# Runs come from runs/VALID.csv. By default: every included threaded-w1 run
# with journal default ON, for the three paths below. Narrow it with the usual
# selection flags (--topology, --run-id, ...). When several runs match one
# path, each is processed on its own and the per-write times are pooled.
SELECTION_DEFAULTS = {"config": ["threaded-w1"], "journal_default": "true"}
ORDER = ["rs-primary-local", "rs-secondary-local", "rs-secondary-majority"]  # top to bottom

LABELS = {
    "rs-primary-local":      ("Primary", "any read"),
    "rs-secondary-local":    ("Secondary", "local read"),
    "rs-secondary-majority": ("Secondary", "majority read"),
}
# (fill, edge, light segment). None means "lighten the fill".
COLOURS = {
    "rs-primary-local":      ("#B4B2A9", "#5F5E5A", None),
    "rs-secondary-local":    ("#5DCAA5", "#0F6E56", None),
    "rs-secondary-majority": ("#EF9F27", "#854F0B", "#FAC775"),
}

# Files inside each run folder (a .gz copy is used if present)
MANIFEST = "manifest.json"
REPL_BEFORE = "provenance/repl_before.json"
WRITES_FILE = "writes.jsonl"
READS_FILE = "reads.jsonl"

# Column names in writes.jsonl and reads.jsonl
W_SEQ, W_START, W_ACK, W_ERR, W_WARM = "seq", "t_start_ns", "t_ack_ns", "error", "warmup"
R_TARGET, R_START, R_END, R_LAT, R_OBS, R_ERR = (
    "target", "t_start_ns", "t_end_ns", "latency_ns", "observed_seq", "error")

NS_PER_MS = 1e6                 # all t_*_ns columns come from perf_counter_ns
READ_TIME_COL = R_END           # END of the first fresh read, as in event_timings.py

# Condition this chart claims
REQUIRED_W = 1
REQUIRED_JOURNAL_DEFAULT = True

# Checks
MIN_WRITES = 1000
P90_SEGMENT_THRESHOLD = 0.05    # draw the p90 segment if p90 > median * 1.05
CLOCK_OVERLAP_SECONDS = 30      # reads and writes must overlap in time within this
# Your earlier rough numbers (ms), used ONLY for the sanity check, never for the chart
CHECK_REFERENCE_MS = {
    "rs-primary-local": (0.6, 0.6),
    "rs-secondary-local": (1.1, 1.1),
    "rs-secondary-majority": (9.0, 13.0),
}

OUT_STEM = "visibility_from_data"
FIG_SIZE = (10, 3.8)


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------
def read_jsonl(run_dir: Path, name: str) -> pd.DataFrame:
    gz = run_dir / (name + ".gz")
    fh = gzip.open(gz, "rt") if gz.exists() else (run_dir / name).open()
    with fh:
        return pd.DataFrame([json.loads(line) for line in fh if line.strip()])


def load_run(run_dir: Path) -> dict:
    missing = []
    if not run_dir.exists():
        raise SystemExit(f"run folder not found: {run_dir}")
    for rel in (MANIFEST, REPL_BEFORE):
        if not (run_dir / rel).exists():
            missing.append(rel)
    for name in (WRITES_FILE, READS_FILE):
        if not (run_dir / name).exists() and not (run_dir / (name + ".gz")).exists():
            missing.append(name + " (or .gz)")
    if missing:
        raise SystemExit(f"{run_dir} is missing: {', '.join(missing)}")
    return {
        "manifest": json.loads((run_dir / MANIFEST).read_text()),
        "repl": json.loads((run_dir / REPL_BEFORE).read_text()),
        "writes": read_jsonl(run_dir, WRITES_FILE),
        "reads": read_jsonl(run_dir, READS_FILE),
    }


# ---------------------------------------------------------------------------
# computation
# ---------------------------------------------------------------------------
def visibility_ms(writes: pd.DataFrame, reads: pd.DataFrame):
    """Per-write visibility time in ms after the write was sent."""
    w_ok = writes[writes[W_ERR].isna()]
    if W_WARM not in w_ok:
        w_ok = w_ok.assign(**{W_WARM: False})
    w = (w_ok[[W_SEQ, W_START, W_WARM]].sort_values(W_START)
         .rename(columns={W_SEQ: "seq", W_START: "w_start", W_WARM: "w_warm"}))

    r = reads[reads[R_ERR].isna()].sort_values(R_START).copy()
    r["observed"] = pd.to_numeric(r[R_OBS], errors="coerce").fillna(-2)

    r = pd.merge_asof(r, w, left_on=R_START, right_on="w_start", direction="backward")
    r = r.dropna(subset=["seq"])
    r = r[~r["w_warm"].astype(bool)]
    r["fresh"] = r["observed"] >= r["seq"]

    first_idx = r[r["fresh"]].groupby("seq")[R_START].idxmin()
    first = r.loc[first_idx]
    vis = pd.Series((first[READ_TIME_COL].to_numpy() - first["w_start"].to_numpy()) / NS_PER_MS,
                    index=first["seq"].astype(int).to_numpy())

    counted = int((~w["w_warm"].astype(bool)).sum())
    counts = {
        "writes_total": len(writes),
        "write_errors": int(writes[W_ERR].notna().sum()),
        "warmup": int(w["w_warm"].astype(bool).sum()),
        "after_exclusions": counted,
        "used": len(vis),
        "never_visible": counted - len(vis),
    }
    return vis, counts


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------
def check_run(key: str, run: dict, issues: list) -> None:
    """Appends (level, message). Levels: ok, warn, fail."""
    m, repl, writes, reads = run["manifest"], run["repl"], run["writes"], run["reads"]
    add = lambda lvl, msg: issues.append((lvl, f"{key}: {msg}"))

    # condition
    w = (m.get("config", {}).get("workload", {}).get("write_concern", {}) or {}).get("w")
    jd = ((repl.get("replSetGetConfig") or {}).get("config") or {}).get(
        "writeConcernMajorityJournalDefault")
    add("ok" if w == REQUIRED_W else "fail", f"write concern w={w!r} (need {REQUIRED_W})")
    add("ok" if jd == REQUIRED_JOURNAL_DEFAULT else "fail",
        f"journal default={jd!r} (need {REQUIRED_JOURNAL_DEFAULT})")

    # topology, for information
    members = (repl.get("replSetGetStatus") or {}).get("members", [])
    sync = {mm["name"][-5:]: (mm.get("syncSourceHost") or "-")[-5:] for mm in members}
    add("ok", f"sync sources at start (node: copies from) {sync}")

    # target label
    targets = set(reads[R_TARGET].dropna().unique())
    add("ok" if targets == {key} else "warn", f"read targets in file: {sorted(targets)}")

    # units: the three time columns must agree and look like nanoseconds
    lat = reads[R_LAT].astype(float)
    ratio = float(np.median((reads[R_END] - reads[R_START]).astype(float) / lat))
    add("ok" if abs(ratio - 1) < 0.01 else "warn",
        f"(t_end - t_start) / latency = {ratio:.3f} (should be 1)")
    med_lat = float(np.median(lat))
    add("ok" if 1e4 <= med_lat <= 1e7 else "warn",
        f"median read latency {med_lat:.0f} ns = {med_lat / NS_PER_MS:.3f} ms "
        f"(plausible if ns and between 0.01 and 10 ms)")
    interval_ms = (m.get("config", {}).get("workload", {}) or {}).get("write_interval_ms")
    gap = float(np.median(np.diff(writes[W_START].to_numpy().astype(float)))) / NS_PER_MS
    if interval_ms:
        add("ok" if abs(gap / interval_ms - 1) < 0.2 else "warn",
            f"median gap between writes {gap:.1f} ms vs configured write_interval_ms {interval_ms}")

    # monotonic counters
    seq_ok = bool((np.diff(writes[W_SEQ].to_numpy()) > 0).all())
    start_ok = bool((np.diff(writes[W_START].to_numpy()) > 0).all())
    add("ok" if seq_ok else "warn", "write seq strictly increasing in file order")
    add("ok" if start_ok else "warn", "write send times strictly increasing in file order")
    gaps = int((np.diff(writes[W_SEQ].to_numpy()) != 1).sum())
    add("ok" if gaps == 0 else "warn", f"gaps in seq numbering: {gaps}")

    # same clock: one process, one clock, so reads and writes must overlap in time
    note = str(m.get("clock_note", ""))
    add("ok" if "perf_counter" in note else "warn",
        f"manifest clock note mentions perf_counter: {'perf_counter' in note}")
    start_off = (reads[R_START].min() - writes[W_START].min()) / 1e9
    end_off = (reads[R_END].max() - writes[W_ACK].max()) / 1e9
    ok = abs(start_off) <= CLOCK_OVERLAP_SECONDS and abs(end_off) <= CLOCK_OVERLAP_SECONDS
    add("ok" if ok else "warn",
        f"first read {start_off:+.2f} s from first write, last read {end_off:+.2f} s "
        f"from last ack (same clock if both are small)")


# ---------------------------------------------------------------------------
# drawing
# ---------------------------------------------------------------------------
def lighten(hex_colour: str, amount: float = 0.55) -> str:
    rgb = [int(hex_colour[i:i + 2], 16) for i in (1, 3, 5)]
    return "#%02X%02X%02X" % tuple(int(c + (255 - c) * amount) for c in rgb)


def has_p90_segment(s: dict) -> bool:
    return s["p90"] > s["median"] * (1 + P90_SEGMENT_THRESHOLD)


def label_text(s: dict) -> str:
    if has_p90_segment(s):
        return f"{s['median']:.1f} to {s['p90']:.1f} ms"
    return f"{s['median']:.1f} ms"


def draw(stats: dict, out_dir: Path) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=FIG_SIZE, facecolor="white")
    ax.set_facecolor("white")
    xmax = max(max(s["p90"], s["median"]) for s in stats.values())
    pad = xmax * 0.012

    for y, key in enumerate(ORDER):
        s = stats[key]
        fill, edge, light = COLOURS[key]
        light = light or lighten(fill)
        ax.barh(y, s["median"], height=0.62, color=fill, edgecolor=edge,
                linewidth=1.5, zorder=3)
        end = s["median"]
        if has_p90_segment(s):
            ax.barh(y, s["p90"] - s["median"], left=s["median"], height=0.62,
                    color=light, edgecolor=edge, linewidth=1.5, zorder=3)
            end = s["p90"]
        ax.text(end + pad, y, label_text(s), va="center", ha="left",
                fontsize=14, fontweight="bold", color=edge, zorder=4)

    ax.set_yticks(range(len(ORDER)))
    ax.set_yticklabels([f"{LABELS[k][0]}\n{LABELS[k][1]}" for k in ORDER], fontsize=13)
    ax.invert_yaxis()
    ax.set_xlim(0, xmax * 1.36)
    ax.set_xlabel("Time until the new value can be read (ms after the write is sent)",
                  fontsize=13)
    ax.tick_params(axis="x", labelsize=12)
    ax.tick_params(axis="y", length=0)
    ax.xaxis.grid(True, color="#E4E4E0", linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    fig.tight_layout()

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / f"{OUT_STEM}.png", out_dir / f"{OUT_STEM}.svg"]
    fig.savefig(paths[0], dpi=300, facecolor="white")
    fig.savefig(paths[1], facecolor="white")
    return paths


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    runs.add_selection_args(ap, defaults={**SELECTION_DEFAULTS, "target": ORDER})
    ap.add_argument("--out", type=Path, default=Path("."), help="where to write the chart files")
    ap.add_argument("--force", action="store_true",
                    help="draw even if the write concern or journal default check fails")
    args = ap.parse_args()

    issues: list = []
    stats: dict = {}
    chosen = runs.selected(args)
    print("Runs used:")
    for key in ORDER:
        matched = [r for r in chosen if r.target == key]
        if not matched:
            raise SystemExit(f"no run in runs/VALID.csv matches the selection for {key}")
        parts, totals = [], {}
        for r in matched:
            print(f"  {key:24s} {r.run_id}  topology={r.row['topology']}  power={r.row['power']}")
            run = load_run(r.path)
            check_run(key, run, issues)
            vis_one, counts = visibility_ms(run["writes"], run["reads"])
            parts.append(vis_one.to_numpy())
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
        vis = np.concatenate(parts)
        q = np.quantile(vis, [0.5, 0.9])
        stats[key] = {"median": float(q[0]), "p90": float(q[1]), "n": len(vis),
                      "runs": len(matched), **totals}
        if len(vis) < MIN_WRITES:
            issues.append(("warn", f"{key}: only {len(vis)} writes used (< {MIN_WRITES})"))

    # compare with the earlier rough numbers
    print("\nComparison with your earlier numbers (sanity check only):")
    for key in ORDER:
        lo, hi = CHECK_REFERENCE_MS[key]
        med = stats[key]["median"]
        mid = (lo + hi) / 2
        if med / mid > 100 or med / mid < 0.01:
            lvl, why = "fail", "off by roughly 1000x or more: probable unit problem"
        elif lo * 0.8 <= med <= hi * 1.2:
            lvl, why = "ok", "within 20% of the earlier number"
        else:
            edge = lo if med < lo else hi
            lvl, why = "warn", f"{abs(med - edge) / edge:.0%} outside the earlier range"
        ref = f"{lo:g}" if lo == hi else f"{lo:g} to {hi:g}"
        print(f"  [{lvl:4s}] {key:24s} computed median {med:7.3f} ms, earlier {ref} ms: {why}")
        if lvl != "ok":
            issues.append((lvl, f"{key}: {why}"))

    print("\nChecks:")
    for lvl, msg in issues:
        print(f"  [{lvl:4s}] {msg}")
    fails = [m for l, m in issues if l == "fail"]
    warns = [m for l, m in issues if l == "warn"]

    print("\nSummary (ms after the write is sent):")
    table = pd.DataFrame([{
        "path": k, "writes_used": stats[k]["n"], "median": round(stats[k]["median"], 3),
        "p90": round(stats[k]["p90"], 3),
        "warmup": stats[k]["warmup"], "write_errors": stats[k]["write_errors"],
        "never_visible": stats[k]["never_visible"]} for k in ORDER])
    print(table.to_string(index=False))

    if fails and not args.force:
        print(f"\n{len(fails)} check(s) FAILED, not drawing. Fix the cause or rerun with --force.")
        sys.exit(2)

    paths = draw(stats, args.out)
    P, L, M = (stats[k] for k in ORDER)
    print("\nWrote: " + ", ".join(str(p) for p in paths))
    print(f"{len(warns)} warning(s), {len(fails)} failure(s).")
    print("\nSlide sentence:")
    print(f"With w:1, a secondary's majority read sees a new write {M['median'] / L['median']:.0f} "
          f"times later than its local read ({M['median']:.1f} against {L['median']:.1f} ms median), "
          f"while the primary is quickest at {P['median']:.1f} ms.")


if __name__ == "__main__":
    main()