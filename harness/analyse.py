#!/usr/bin/env python3
"""Turn a raw run directory into freshness curves and summary tables.

Version 2. Changes from v1:

  * In-flight reads are no longer discarded. With independent threads, many
    reads land between a write's submit and its acknowledgement, and those are
    exactly the reads v1 could never collect. They are kept for the
    from-write-start curve (where the anchor is well defined) and excluded
    from the from-ack curve (where the ack has not happened yet).
  * Error rates per target are reported instead of silently dropped.
  * Read latency distributions are reported, which is where a causal read
    should show its cost: it trades staleness for waiting.
  * Several run directories can be given at once and are analysed as one
    sample. Isolated runs measure one target per process, so a full picture of
    every target now lives across several directories rather than one.

Nothing here writes back into the run's raw files.

Definitions
-----------
A read R is FRESH with respect to write W if it returns W's sequence number,
where W is the most recent write preceding R. Two anchors, both computed:

    elapsed_from_start = R.t_start - W.t_start
    elapsed_from_ack   = R.t_start - W.t_ack

LAG is (current write seq) minus (observed seq): how many writes behind.

Runs are selected from runs/VALID.csv, never by globbing measurements/.

Usage:
    python -m harness.analyse --plan runs/plans/<stamp>__recollect.json
    python -m harness.analyse --run-id <run_id>
    python -m harness.analyse --run-id <run_id> --bucket-us 2000 --min-n 5
    python -m harness.analyse --config threaded-w1 --topology star
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import runs


def _read_jsonl(run_dir: Path, name: str) -> pd.DataFrame:
    plain, gz = run_dir / name, run_dir / (name + ".gz")
    if gz.exists():
        with gzip.open(gz, "rt") as fh:
            rows = [json.loads(line) for line in fh]
    elif plain.exists():
        rows = [json.loads(line) for line in plain.open()]
    else:
        raise FileNotFoundError(f"{name} not found in {run_dir}")
    return pd.DataFrame(rows)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval. The normal approximation is useless here because
    p sits at 0 or 1 in most buckets."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def join(reads: pd.DataFrame, writes: pd.DataFrame) -> pd.DataFrame:
    """Attach to each read the most recent write that preceded it.

    Two joins: one on write acknowledgement, one on write submission. When
    they disagree, a write was in flight at the moment of the read. Those reads
    are flagged, not dropped: with independent threads they are a large and
    genuinely interesting share of the sample.
    """
    w = writes.sort_values("t_ack_ns")[["seq", "t_start_ns", "t_ack_ns",
                                        "latency_ns"]].rename(
        columns={"seq": "acked_seq", "t_start_ns": "w_start_ns",
                 "t_ack_ns": "w_ack_ns", "latency_ns": "w_latency_ns"})
    s = writes.sort_values("t_start_ns")[["seq", "t_start_ns"]].rename(
        columns={"seq": "submitted_seq", "t_start_ns": "sub_start_ns"})

    r = reads.sort_values("t_start_ns").copy()
    df = pd.merge_asof(r, w, left_on="t_start_ns", right_on="w_ack_ns",
                       direction="backward")
    df = pd.merge_asof(df, s, left_on="t_start_ns", right_on="sub_start_ns",
                       direction="backward")

    df["in_flight"] = df["submitted_seq"] != df["acked_seq"]
    df["elapsed_from_ack_ns"] = df["t_start_ns"] - df["w_ack_ns"]
    df["elapsed_from_start_ns"] = df["t_start_ns"] - df["sub_start_ns"]

    # Fresh is judged against the most recently SUBMITTED write: a read during
    # the write window that already sees the new value is fresh, and that is a
    # real observation the cycle-based harness could not make.
    df["fresh"] = df["observed_seq"] == df["submitted_seq"]
    df["fresh_vs_acked"] = df["observed_seq"] == df["acked_seq"]
    df["lag_writes"] = df["submitted_seq"] - df["observed_seq"]
    return df


def curve(df: pd.DataFrame, bucket_us: int, min_n: int,
          anchor: str = "ack") -> pd.DataFrame:
    col = f"elapsed_from_{anchor}_ns"
    d = df[df["error"].isna() & ~df["warmup"]].copy()
    if anchor == "ack":
        # The acknowledgement has not happened for an in-flight read, so the
        # anchor is undefined. Judge against the acked write.
        d = d[~d["in_flight"]]
        d["is_fresh"] = d["fresh_vs_acked"]
    else:
        d["is_fresh"] = d["fresh"]
    d = d[d[col] >= 0]
    d["bucket_us"] = (d[col] // (bucket_us * 1000)) * bucket_us

    g = (d.groupby(["target", "bucket_us"])["is_fresh"]
           .agg(["sum", "count"])
           .rename(columns={"sum": "n_fresh", "count": "n"})
           .reset_index())
    g["p_fresh"] = g["n_fresh"] / g["n"]
    ci = g.apply(lambda r: wilson(int(r["n_fresh"]), int(r["n"])), axis=1)
    g["ci_low"] = [c[0] for c in ci]
    g["ci_high"] = [c[1] for c in ci]
    g["adequate"] = g["n"] >= min_n
    return g.sort_values(["target", "bucket_us"])


def headline(g: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target, sub in g[g["adequate"]].groupby("target"):
        sub = sub.sort_values("bucket_us")
        over99 = sub[sub["p_fresh"] >= 0.99]
        under100 = sub[sub["p_fresh"] < 1.0]
        rows.append({
            "target": target,
            "n_reads": int(sub["n"].sum()),
            "p_fresh_overall": sub["n_fresh"].sum() / sub["n"].sum(),
            "first_bucket_us_p99": int(over99["bucket_us"].iloc[0]) if len(over99) else None,
            "last_bucket_us_below_1": int(under100["bucket_us"].iloc[-1]) if len(under100) else None,
        })
    return pd.DataFrame(rows)


def errors(df: pd.DataFrame) -> pd.DataFrame:
    """Failures are data. Under load MongoDB returns errors, and which ones
    and how often is part of the result."""
    rows = []
    for target, sub in df[~df["warmup"]].groupby("target"):
        failed = sub[sub["error"].notna()]
        kinds = failed["error"].str.split(":").str[0].value_counts().to_dict()
        rows.append({"target": target, "reads": len(sub),
                     "errors": len(failed),
                     "error_rate": len(failed) / len(sub) if len(sub) else 0.0,
                     "kinds": json.dumps(kinds)})
    return pd.DataFrame(rows)


def latency(df: pd.DataFrame) -> pd.DataFrame:
    """A causal read trades staleness for waiting, so its latency is the other
    half of the story and belongs beside the freshness numbers."""
    d = df[df["error"].isna() & ~df["warmup"]]
    q = d.groupby("target")["latency_ns"].quantile([0.5, 0.95, 0.99]).unstack()
    q.columns = ["p50_ns", "p95_ns", "p99_ns"]
    q["mean_ns"] = d.groupby("target")["latency_ns"].mean()
    q["max_ns"] = d.groupby("target")["latency_ns"].max()
    return q.reset_index()


def monotonic_violations(df: pd.DataFrame) -> pd.DataFrame:
    """Consecutive reads on one target where the observed value goes backwards.
    The MongoDB analogue of CIDR Table 2.

    Grouped by run as well as by target: two reads either side of a run
    boundary are not consecutive reads, and comparing across it would invent
    regressions out of nothing.
    """
    rows = []
    for (_run_id, target), sub in df[~df["warmup"]].groupby(["run_id", "target"]):
        sub = sub.sort_values("t_start_ns")
        prev = sub["observed_seq"].shift(1)
        valid = prev.notna() & sub["observed_seq"].notna()
        regress = valid & (sub["observed_seq"] < prev)
        rows.append({"target": target, "pairs": int(valid.sum()),
                     "violations": int(regress.sum())})
    if not rows:
        return pd.DataFrame(columns=["target", "pairs", "violations", "rate"])
    out = (pd.DataFrame(rows)
             .groupby("target", as_index=False)[["pairs", "violations"]].sum())
    out["rate"] = [v / n if n else float("nan")
                   for v, n in zip(out["violations"], out["pairs"])]
    return out


def plot(g: pd.DataFrame, out: Path, anchor: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    for target, sub in g[g["adequate"]].groupby("target"):
        sub = sub.sort_values("bucket_us")
        ax.plot(sub["bucket_us"] / 1000.0, sub["p_fresh"], label=target, lw=1.2)
        ax.fill_between(sub["bucket_us"] / 1000.0, sub["ci_low"],
                        sub["ci_high"], alpha=0.15, linewidth=0)
    ax.set_xlabel(f"time elapsed from write {anchor} until read start (ms)")
    ax.set_ylabel("P(read observes freshest value)")
    ax.set_ylim(-0.02, 1.02)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)


def load_run(run_dir: Path) -> tuple[dict, pd.DataFrame]:
    """Join one run's reads to its own writes, tagged with where they came from.

    The join MUST happen per run and never on pooled frames. t_*_ns is
    perf_counter_ns(), whose epoch is per process, so a read from one run and a
    write from another share no clock; merging the raw records first would pair
    them anyway and produce plausible nonsense.
    """
    manifest = json.loads((run_dir / "manifest.json").read_text())
    if "mode" not in manifest:
        raise SystemExit(f"[analyse] {run_dir} has no mode in its manifest, so "
                         f"it predates this harness version and cannot be "
                         f"analysed here.")
    df = join(_read_jsonl(run_dir, "reads.jsonl"),
              _read_jsonl(run_dir, "writes.jsonl"))
    df["run_id"] = manifest["run_id"]
    df["mode"] = manifest["mode"]
    df["round_index"] = manifest.get("round_index", 1)
    return manifest, df


# --------------------------------------------------------------------------
# re-collect figures (--plan)
# --------------------------------------------------------------------------
#
# The pooled curves above group by target only, which is right for one config
# and wrong for a plan that spans several: rs-primary-local under w:1 and under
# w:"majority" would become one line. Plan mode summarises each run on its own,
# keeps config as part of every key, and draws figures for the cells of the
# plan. Every figure has a CSV twin with the numbers it shows.

INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
# Reference categorical slots 1-4, in order (validated adjacent, light mode).
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]

CONFIG_ORDER = ["threaded-w1", "threaded-w2", "threaded-w3", "threaded-wmaj"]
WC_LABEL = {"threaded-w1": "w:1", "threaded-w2": "w:2", "threaded-w3": "w:3",
            "threaded-wmaj": 'w:"majority"'}
TARGET_ORDER = ["rs-primary-local", "rs-primary-linearizable",
                "rs-secondary-local", "rs-secondary-majority",
                "rs-secondary-causal-local", "rs-secondary-causal-majority",
                "direct-27017", "direct-27018", "direct-27019", "direct-27020"]
# Paths drawn in colour on the freshness curves; the rest are grey context.
HIGHLIGHT = [("rs-secondary-majority", "Secondary, majority read", SLOTS[0]),
             ("rs-secondary-local", "Secondary, local read", SLOTS[1]),
             ("rs-secondary-causal-majority", "Secondary, causal majority read", SLOTS[2])]

_READ_COLS = ["target", "t_start_ns", "latency_ns", "observed_seq", "error",
              "warmup", "missed_slots"]


def _summarise_run(run: runs.Run, bucket_us: int) -> dict:
    """Everything the figures need from one run, joined on the run's own clock."""
    m = run.manifest
    reads = _read_jsonl(run.path, "reads.jsonl")[_READ_COLS]
    writes = _read_jsonl(run.path, "writes.jsonl")
    df = join(reads, writes)
    ok = df[df["error"].isna() & ~df["warmup"]]
    j = ok.dropna(subset=["acked_seq"])
    w_ok = writes[writes["error"].isna() & ~writes["warmup"]]
    curves = {}
    for anchor in ("start", "ack"):
        g = curve(df, bucket_us, 1, anchor)[["target", "bucket_us", "n_fresh", "n"]]
        g.insert(0, "config", run.row["config"])
        curves[anchor] = g
    return {"config": run.row["config"], "target": run.target,
            "round": m.get("round_index", 1), "run_id": run.run_id,
            "read_ns": ok["latency_ns"].to_numpy(),
            "ack_ns": w_ok["latency_ns"].to_numpy(),
            # stale: older than the latest write acked before the read started
            "stale": int((j["observed_seq"].fillna(-1) < j["acked_seq"]).sum()),
            "stale_of": len(j),
            "r_missed": int(reads["missed_slots"].sum()),
            "w_missed": int(writes["missed_slots"].sum()),
            "curves": curves}


def _q(a, q):
    import numpy as np
    return float(np.quantile(a, q)) / 1e6 if len(a) else float("nan")


def _cell_key(c):
    return (CONFIG_ORDER.index(c["config"]) if c["config"] in CONFIG_ORDER else 99,
            TARGET_ORDER.index(c["target"]) if c["target"] in TARGET_ORDER else 99)


def _cells(summaries: list[dict]) -> list[dict]:
    import numpy as np
    by = {}
    for x in summaries:
        by.setdefault((x["config"], x["target"]), []).append(x)
    out = []
    for (cfg, tgt), xs in by.items():
        rl = np.concatenate([x["read_ns"] for x in xs])
        al = np.concatenate([x["ack_ns"] for x in xs])
        st, of = sum(x["stale"] for x in xs), sum(x["stale_of"] for x in xs)
        out.append({"config": cfg, "target": tgt, "runs": len(xs),
                    "stale": st, "stale_of": of,
                    "stale_rate": st / of if of else float("nan"),
                    "read_p50_ms": _q(rl, .5), "read_p90_ms": _q(rl, .9),
                    "read_p99_ms": _q(rl, .99),
                    "ack_p50_ms": _q(al, .5), "ack_p90_ms": _q(al, .9),
                    "ack_p99_ms": _q(al, .99),
                    "r_missed": sum(x["r_missed"] for x in xs),
                    "w_missed": sum(x["w_missed"] for x in xs)})
    return sorted(out, key=_cell_key)


def _pool_curves(summaries: list[dict], anchor: str, min_n: int) -> pd.DataFrame:
    g = (pd.concat([x["curves"][anchor] for x in summaries])
           .groupby(["config", "target", "bucket_us"], as_index=False)[["n_fresh", "n"]]
           .sum())
    g["p_fresh"] = g["n_fresh"] / g["n"]
    ci = [wilson(int(k), int(n)) for k, n in zip(g["n_fresh"], g["n"])]
    g["ci_low"] = [c[0] for c in ci]
    g["ci_high"] = [c[1] for c in ci]
    g["adequate"] = g["n"] >= min_n
    return g.sort_values(["config", "target", "bucket_us"])


def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9,
        "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
        "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
        "xtick.labelcolor": INK2, "ytick.labelcolor": INK2,
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE, "legend.frameon": False,
    })
    return plt


def _style(ax, grid_axis="y"):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8, linestyle="-")
    ax.set_axisbelow(True)
    ax.tick_params(length=3, width=0.8)


def _log_ticks(ax, lo, hi):
    """Plain millisecond ticks (0.1, 0.2, 0.5, 1, 2, 5, ...) on a log axis."""
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
    ticks = [m * 10 ** e for e in range(-2, 4) for m in (1, 2, 5)
             if lo <= m * 10 ** e <= hi]
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))


def _fig_freshness(g: pd.DataFrame, anchor: str, bucket_us: int,
                   xmax_ms: float, out: Path) -> Path:
    plt = _plt()
    from matplotlib.lines import Line2D
    configs = [c for c in ("threaded-w1", "threaded-wmaj") if c in set(g["config"])]
    fig, axes = plt.subplots(1, len(configs), figsize=(5.0 * len(configs), 3.6),
                             sharey=True, squeeze=False)
    hl = {t for t, _, _ in HIGHLIGHT}
    for ax, cfg in zip(axes[0], configs):
        grid = range(0, int(xmax_ms * 1000), bucket_us)
        sub = {}
        for tgt, s in g[g["config"] == cfg].groupby("target"):
            # Full bucket grid: a bucket that is missing or under the minimum
            # sample becomes NaN, so the line breaks instead of bridging it.
            s = s.set_index("bucket_us").reindex(grid)
            bad = ~s["adequate"].fillna(False).astype(bool)
            s.loc[bad, ["p_fresh", "ci_low", "ci_high"]] = float("nan")
            sub[tgt] = s
        for tgt, s in sub.items():
            if tgt not in hl:
                ax.plot((s.index + bucket_us / 2) / 1000, s["p_fresh"],
                        color=AXIS, lw=1, zorder=1, solid_capstyle="round")
        for tgt, _, color in HIGHLIGHT:
            s = sub.get(tgt)
            if s is None or s["p_fresh"].isna().all():
                continue
            x = (s.index + bucket_us / 2) / 1000
            ax.fill_between(x, s["ci_low"], s["ci_high"], color=color,
                            alpha=0.12, lw=0, zorder=2)
            ax.plot(x, s["p_fresh"], color=color, lw=2, zorder=3,
                    solid_capstyle="round", solid_joinstyle="round")
        ax.set_title(WC_LABEL[cfg], loc="left", color=INK, fontweight="semibold")
        ax.set_xlim(0, xmax_ms)
        ax.set_ylim(-0.02, 1.02)
        _style(ax, "both")
        ax.set_xlabel("Time since the write was "
                      + ("sent" if anchor == "start" else "acknowledged") + " (ms)")
    axes[0][0].set_ylabel("Share of reads returning the newest write")
    handles = [Line2D([], [], color=c, lw=2, label=lab) for _, lab, c in HIGHLIGHT]
    handles.append(Line2D([], [], color=AXIS, lw=1, label="Other paths"))
    fig.legend(handles=handles, loc="upper left", ncol=4, fontsize=8,
               bbox_to_anchor=(0.01, 1.0), labelcolor=INK2)
    note = ("Newest write = the one most recently sent; reads during the write window count."
            if anchor == "start" else
            "Newest write = the one most recently acknowledged; reads while a write was in flight are excluded.")
    fig.text(0.01, 0.005, f"{note} Shaded: 95% Wilson interval. "
             f"{bucket_us} us buckets; gaps are buckets under the minimum sample. "
             f"Lines at 1.0 overlap.", color=MUTED, fontsize=7)
    fig.tight_layout(rect=(0, 0.04, 1, 0.92))
    path = out / f"freshness_from_{anchor}.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    return path


def _row_labels(cells):
    return [f"{WC_LABEL.get(c['config'], c['config'])}   {c['target']}" for c in cells]


def _group_rules(ax, cells):
    for i in range(1, len(cells)):
        if cells[i]["config"] != cells[i - 1]["config"]:
            ax.axhline(i - 0.5, color=AXIS, lw=0.8, zorder=0)


def _fig_stale(cells: list[dict], out: Path) -> Path:
    plt = _plt()
    n = len(cells)
    fig, ax = plt.subplots(figsize=(8.6, 0.28 * n + 1.3))
    rates = [100 * c["stale_rate"] for c in cells]
    ax.barh(range(n), rates, height=0.55, color=SLOTS[0], zorder=2)
    top = max(rates) or 1
    for i, (c, r) in enumerate(zip(cells, rates)):
        lab = (f"{r:.3g}%  ({c['stale']:,} of {c['stale_of']:,})" if c["stale"]
               else f"0  (of {c['stale_of']:,})")
        ax.text(r + top * 0.01, i, lab, va="center", fontsize=7.5, color=INK2)
    ax.set_yticks(range(n))
    ax.set_yticklabels(_row_labels(cells))
    ax.invert_yaxis()
    ax.set_xlim(0, top * 1.45)
    _group_rules(ax, cells)
    _style(ax, "x")
    ax.set_xlabel("Stale reads (% of reads)")
    ax.set_title("Stale reads per cell", loc="left", color=INK, fontweight="semibold")
    fig.text(0.01, 0.005, "Stale = returned a value older than the latest write "
             "acknowledged before the read started. Both rounds pooled; warmup excluded.",
             color=MUTED, fontsize=7)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    path = out / "stale_rate_by_cell.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    return path


def _fig_latency(cells: list[dict], out: Path) -> Path:
    plt = _plt()
    n = len(cells)
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 0.28 * n + 1.5), sharey=True)
    for ax, key, title in ((axes[0], "read", "Read latency (ms)"),
                           (axes[1], "ack", "Write acknowledgement latency (ms)")):
        for i, c in enumerate(cells):
            p50, p90, p99 = c[f"{key}_p50_ms"], c[f"{key}_p90_ms"], c[f"{key}_p99_ms"]
            ax.hlines(i, p50, p99, color=SLOTS[0], lw=2, zorder=2)
            ax.plot(p90, i, marker="|", ms=8, mew=1.5, color=SLOTS[0], zorder=3)
            ax.plot(p50, i, "o", ms=7, color=SLOTS[0], mec=SURFACE, mew=1.5, zorder=4)
        ax.set_xscale("log")
        lo = min(c[f"{key}_p50_ms"] for c in cells)
        hi = max(c[f"{key}_p99_ms"] for c in cells)
        ax.set_xlim(lo / 1.4, hi * 1.4)
        _log_ticks(ax, lo / 1.4, hi * 1.4)
        ax.set_title(title, loc="left", color=INK, fontweight="semibold")
        _group_rules(ax, cells)
        _style(ax, "x")
    axes[0].set_yticks(range(n))
    axes[0].set_yticklabels(_row_labels(cells))
    axes[0].invert_yaxis()
    fig.text(0.01, 0.005, "Dot = median, tick = p90, line ends at p99. Log scale. "
             "Both rounds pooled; warmup and errors excluded.", color=MUTED, fontsize=7)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    path = out / "latency_by_cell.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    return path


def _fig_ack_cdf(summaries: list[dict], out: Path) -> tuple[Path, pd.DataFrame]:
    import numpy as np
    plt = _plt()
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    rows = []
    slot = 0
    for cfg in CONFIG_ORDER:
        xs = [x["ack_ns"] for x in summaries
              if x["config"] == cfg and x["target"] == "rs-primary-local"]
        if not xs:
            continue
        a = np.sort(np.concatenate(xs)) / 1e6
        y = np.arange(1, len(a) + 1) / len(a)
        p50, p99 = np.quantile(a, .5), np.quantile(a, .99)
        ax.plot(a, y, color=SLOTS[slot], lw=2, solid_capstyle="round",
                label=f"{WC_LABEL[cfg]}   median {p50:.2f} ms, p99 {p99:.2f} ms")
        rows.append({"write_concern": WC_LABEL[cfg], "config": cfg, "n": len(a),
                     "p01_ms": np.quantile(a, .01), "p50_ms": p50,
                     "p90_ms": np.quantile(a, .9), "p99_ms": p99, "max_ms": a[-1]})
        slot += 1
    ax.set_xscale("log")
    _log_ticks(ax, *ax.get_xlim())
    ax.set_ylim(0, 1.01)
    ax.set_xlabel("Write acknowledgement latency (ms, log scale)")
    ax.set_ylabel("Share of writes acknowledged")
    ax.set_title("Write acknowledgement by write concern", loc="left",
                 color=INK, fontweight="semibold")
    ax.legend(loc="lower right", fontsize=8, labelcolor=INK2)
    _style(ax, "both")
    fig.text(0.01, 0.005, "rs-primary-local cells only, both rounds pooled, warmup "
             "excluded. Writes every 100 ms. w:2 and w:3 nearly coincide.",
             color=MUTED, fontsize=7)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    path = out / "ack_cdf_by_write_concern.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    return path, pd.DataFrame(rows)


def recollect_figures(chosen: list, plan: Path, out: Path | None,
                      bucket_us: int | None, min_n: int | None,
                      xmax_ms: float) -> None:
    acfg = chosen[0].manifest["config"].get("analysis", {})
    bucket_us = bucket_us or acfg.get("bucket_us", 500)
    min_n = min_n or acfg.get("min_samples_per_bucket", 30)
    out = out or (runs.MEASUREMENTS / "merged"
                  / f"{plan.name.split('__')[0]}__recollect_figures")
    out.mkdir(parents=True, exist_ok=True)

    summaries = []
    for i, run in enumerate(chosen, 1):
        print(f"[analyse] {i}/{len(chosen)} {run.run_id}")
        summaries.append(_summarise_run(run, bucket_us))

    cells = _cells(summaries)
    pd.DataFrame(cells).to_csv(out / "cells.csv", index=False)
    written = []
    for anchor in ("start", "ack"):
        g = _pool_curves(summaries, anchor, min_n)
        g.to_csv(out / f"curve_{anchor}.csv", index=False)
        written.append(_fig_freshness(g, anchor, bucket_us, xmax_ms, out))
    written.append(_fig_stale(cells, out))
    written.append(_fig_latency(cells, out))
    path, ack = _fig_ack_cdf(summaries, out)
    ack.to_csv(out / "ack_by_write_concern.csv", index=False)
    written.append(path)
    pd.DataFrame([{"run_id": x["run_id"], "config": x["config"],
                   "target": x["target"], "round": x["round"]} for x in summaries]
                 ).to_csv(out / "sources.csv", index=False)
    print(f"\n[analyse] {len(summaries)} runs, {len(cells)} cells, "
          f"bucket {bucket_us} us, min n {min_n}")
    for p_ in written:
        print(f"  {p_}")
    print(f"  tables: cells.csv, curve_start.csv, curve_ack.csv, "
          f"ack_by_write_concern.csv, sources.csv in {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    runs.add_selection_args(ap)
    ap.add_argument("--out", type=Path, default=None,
                    help="where derived outputs go; defaults to "
                         "<run_dir>/derived for a single run and to "
                         "measurements/merged/<stamp>__<name> for a merge")
    ap.add_argument("--bucket-us", type=int, default=None)
    ap.add_argument("--min-n", type=int, default=None)
    ap.add_argument("--plan", type=Path, default=None,
                    help="a runs/plans/<stamp>__recollect.json: draw the "
                         "re-collect figures for the runs it completed")
    ap.add_argument("--xmax-ms", type=float, default=20.0,
                    help="x range of the freshness curves in plan mode")
    args = ap.parse_args()

    if args.plan:
        args.run_id, args.config = runs.plan_run_ids(args.plan), None
        recollect_figures(runs.selected(args), args.plan, args.out,
                          args.bucket_us, args.min_n, args.xmax_ms)
        return

    run_dirs = [r.path for r in runs.selected(args, require_filter=True)]
    loaded = [load_run(d) for d in run_dirs]
    manifests = [m for m, _ in loaded]

    # Isolated and concurrent runs are not the same measurement: concurrent
    # runs carry the latency cost of every other reader in the process. Pooling
    # them would average two different experiments into one curve.
    if len({m["mode"] for m in manifests}) > 1:
        listing = "\n  ".join(f"{m['mode']:<10} {d}"
                              for m, d in zip(manifests, run_dirs))
        raise SystemExit(f"[analyse] refusing to merge isolated and concurrent "
                         f"runs into one curve:\n  {listing}")

    acfg = manifests[0]["config"].get("analysis", {})
    bucket_us = args.bucket_us or acfg.get("bucket_us", 500)
    min_n = args.min_n or acfg.get("min_samples_per_bucket", 30)
    if len({json.dumps(m["config"].get("analysis", {}), sort_keys=True)
            for m in manifests}) > 1:
        print(f"[analyse] WARNING runs disagree on analysis settings; using "
              f"bucket_us={bucket_us}, min_n={min_n} from "
              f"{manifests[0]['run_id']}")

    df = pd.concat([d for _, d in loaded], ignore_index=True)

    if len(loaded) == 1:
        out = args.out or (run_dirs[0] / "derived")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = manifests[0]["config"]["experiment"]["name"]
        out = args.out or (run_dirs[0].parent / "merged" /
                           f"{stamp}__{name}")
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "reads_joined.parquet", index=False)

    if len(loaded) > 1:
        pd.DataFrame([{"run_id": m["run_id"], "mode": m["mode"],
                       "target": m.get("target"),
                       "round_index": m.get("round_index", 1),
                       "path": str(d)}
                      for m, d in zip(manifests, run_dirs)]
                     ).to_csv(out / "sources.csv", index=False)

    for anchor in ("ack", "start"):
        g = curve(df, bucket_us, min_n, anchor=anchor)
        g.to_csv(out / f"curve_{anchor}.csv", index=False)
        headline(g).to_csv(out / f"headline_{anchor}.csv", index=False)
        plot(g, out / f"freshness_curve_{anchor}.png", anchor)

    errors(df).to_csv(out / "errors.csv", index=False)
    latency(df).to_csv(out / "read_latency.csv", index=False)
    monotonic_violations(df).to_csv(out / "monotonic_reads.csv", index=False)
    df[~df["warmup"]].groupby("target")["lag_writes"].describe().to_csv(
        out / "lag_writes.csv")

    inflight = df[~df["warmup"]]["in_flight"].mean()
    if len(loaded) > 1:
        print(f"[analyse] merged {len(loaded)} runs, mode="
              f"{manifests[0]['mode']}")
    print(headline(curve(df, bucket_us, min_n, "ack")).to_string(index=False))
    print(f"\nin-flight reads: {inflight:.1%} of sample "
          f"(invisible to the v1 cycle harness)")
    print(errors(df).to_string(index=False))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
