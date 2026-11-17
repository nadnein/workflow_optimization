#!/usr/bin/env python3
"""
Select candidate modules for resource optimisation from the per-task table written by scripts/megatest_waste.py.
Reads a tasks__*.parquet only (no S3 access); by default the newest one in --out.

One row per module with a metric and a yes/no flag for each criterion:
  1. multi_pipeline  module is used in at least --min-pipelines pipelines
  2. failures        FAILED-task rate or retry rate (attempt > 1) of at least --min-fail-rate
  3. runtime         median task runtime >= --min-median-runtime-min, or total runtime >= --min-total-runtime-h
  4. inefficient     median CPU efficiency (used / requested) <= --max-cpu-eff, or memory that could be cut
                     (median request minus 95th percentile of peak use) >= --min-mem-slack-gb. Memory and walltime
                     efficiency are not flagged directly: nearly every module uses <10 % of the default request
  5. input_sensitive resource use rises with input size: Spearman correlation of peak_rss and of runtime with
                     (a) rchar, the bytes a task read through read calls (available for nearly all tasks), and
                     (b) the real input size of the task's sample from scripts/input_sizes.py (only where a samplesheet
                     could be resolved). Computed per pipeline (each needs >= --min-corr-tasks tasks and at least
                     3 distinct input values) and averaged over pipelines; flag if any of them >= --min-corr.
A module is a `candidate` if it passes criterion 1, has at least --min-tasks tasks and meets at least
--min-other of the other four criteria.

Usage:
  uv run python scripts/module_candidates.py
  uv run python scripts/module_candidates.py --min-pipelines 3 --max-cpu-eff 0.4 --min-other 3
"""
import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd


def _corr_by_pipeline(ok, xcol, name, min_corr_tasks):
    """Per module: mean over pipelines of the Spearman correlation of xcol with peak_rss and with realtime."""
    data = ok[ok[xcol] > 0]
    warnings.simplefilter("ignore", RuntimeWarning)  # constant columns give NaN correlations, which is intended
    rows = {}
    for (module, _), g in data.groupby(["module", "pipeline"]):
        if len(g) >= min_corr_tasks and g[xcol].nunique() >= 3:
            r = g[[xcol, "peak_rss", "realtime", "cpu_time_h"]].rank()  # Spearman = Pearson correlation of the ranks
            rows.setdefault(module, []).append((r[xcol].corr(r["peak_rss"]), r[xcol].corr(r["realtime"]),
                                                r[xcol].corr(r["cpu_time_h"])))
    return pd.DataFrame({m: np.nanmean(v, axis=0) for m, v in rows.items()},
                        index=[f"corr_{name}_rss", f"corr_{name}_runtime", f"corr_{name}_cpu"]).T


def build(df, a, input_sizes=None):
    done = df[df["status"].isin(["COMPLETED", "CACHED", "FAILED"])]  # ABORTED is mostly collateral, not counted
    ok = df[df["status"].isin(["COMPLETED", "CACHED"])].dropna(subset=["peak_rss", "%cpu", "realtime"]).copy()
    if input_sizes is not None:
        # real input size of the sample a task processed: task tag == samplesheet sample, only fully sized samples
        sizes = input_sizes[input_sizes["n_sized"] == input_sizes["n_files"]].rename(columns={"sample": "tag", "input_bytes": "input_bytes"})
        ok = ok.merge(sizes[["pipeline", "run", "tag", "input_bytes"]], on=["pipeline", "run", "tag"], how="left")
    else:
        ok["input_bytes"] = np.nan
    ok["cpu_time_h"] = ok["realtime"] / 3.6e6 * ok["%cpu"] / 100  # core-hours actually used
    ok["cpu_eff"] = ok["%cpu"] / 100 / ok["cpus"]
    ok["mem_eff"] = ok["peak_rss"] / ok["memory"]
    ok["time_eff"] = ok["realtime"] / ok["time"]  # time = requested walltime in ms

    g = ok.groupby("module")
    out = pd.DataFrame({
        "pipelines": g["pipeline"].nunique(),
        "tasks": g.size(),
        "failure_rate": (done["status"] == "FAILED").groupby(done["module"]).mean(),
        "retry_rate": (done["attempt"] > 1).groupby(done["module"]).mean(),
        "median_runtime_min": g["realtime"].median() / 6e4,
        "total_runtime_h": g["realtime"].sum() / 3.6e6,
        "cpu_eff_median": g["cpu_eff"].median(),
        "mem_eff_median": g["mem_eff"].median(),
        "req_mem_gb_median": g["memory"].median() / 1e9,
        "peak_rss_gb_p95": g["peak_rss"].quantile(0.95) / 1e9,
        "time_eff_median": g["time_eff"].median(),
        "rss_p95_over_p50": g["peak_rss"].quantile(0.95) / g["peak_rss"].median(),
        "runtime_p95_over_p50": g["realtime"].quantile(0.95) / g["realtime"].median(),
    }).join(_corr_by_pipeline(ok, "rchar", "rchar", a.min_corr_tasks)).join(_corr_by_pipeline(ok, "input_bytes", "input", a.min_corr_tasks))
    # how wide the input sizes are that the correlation rests on: a strong correlation over a narrow range proves little
    inp = ok[ok["input_bytes"] > 0].groupby("module")["input_bytes"]
    out["input_gb_min"] = inp.min() / 1e9
    out["input_gb_max"] = inp.max() / 1e9
    out["input_distinct_sizes"] = inp.nunique()
    out["mem_slack_gb"] = (out["req_mem_gb_median"] - out["peak_rss_gb_p95"]).clip(lower=0)
    out["failure_rate"] = out["failure_rate"].fillna(0)
    out["retry_rate"] = out["retry_rate"].fillna(0)

    out["multi_pipeline"] = out["pipelines"] >= a.min_pipelines
    out["failures"] = (out["failure_rate"] >= a.min_fail_rate) | (out["retry_rate"] >= a.min_fail_rate)
    out["runtime"] = (out["median_runtime_min"] >= a.min_median_runtime_min) | (out["total_runtime_h"] >= a.min_total_runtime_h)
    out["inefficient"] = (out["cpu_eff_median"] <= a.max_cpu_eff) | (out["mem_slack_gb"] >= a.min_mem_slack_gb)
    out["input_sensitive"] = out[[c for c in out.columns if c.startswith("corr_")]].max(axis=1) >= a.min_corr
    others = ["failures", "runtime", "inefficient", "input_sensitive"]
    out["criteria_met"] = out[["multi_pipeline"] + others].sum(axis=1)
    out["candidate"] = out["multi_pipeline"] & (out["tasks"] >= a.min_tasks) & (out[others].sum(axis=1) >= a.min_other)
    return out.sort_values(["candidate", "criteria_met", "total_runtime_h"], ascending=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, help="tasks parquet to use (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    ap.add_argument("--input-sizes", type=Path, help="input_sizes parquet (default: the one matching the tasks parquet in --out)")
    ap.add_argument("--min-pipelines", type=int, default=2)
    ap.add_argument("--min-tasks", type=int, default=10)
    ap.add_argument("--min-fail-rate", type=float, default=0.02)
    ap.add_argument("--min-median-runtime-min", type=float, default=10)
    ap.add_argument("--min-total-runtime-h", type=float, default=100)
    ap.add_argument("--max-cpu-eff", type=float, default=0.3)
    ap.add_argument("--min-mem-slack-gb", type=float, default=16)
    ap.add_argument("--min-corr", type=float, default=0.5)
    ap.add_argument("--min-corr-tasks", type=int, default=20)
    ap.add_argument("--min-other", type=int, default=2, help="how many of criteria 2-5 a candidate must meet")
    a = ap.parse_args()

    data = a.data or max(a.out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        raise SystemExit(f"No tasks__*.parquet in {a.out}; run scripts/megatest_waste.py first")
    sizes_file = a.input_sizes or (a.out / f"input_sizes__{data.stem.removeprefix('tasks__')}.parquet")
    input_sizes = pd.read_parquet(sizes_file) if sizes_file.exists() else None
    print(f"input sizes: {sizes_file if input_sizes is not None else 'none found (run scripts/input_sizes.py); using rchar only'}")
    out = build(pd.read_parquet(data), a, input_sizes)
    # file name carries the data tag and every threshold used
    settings = (f"pipelines-{a.min_pipelines}_tasks-{a.min_tasks}_fail-{a.min_fail_rate}_runtime-{a.min_median_runtime_min}min-"
                f"{a.min_total_runtime_h}h_cpueff-{a.max_cpu_eff}_memslack-{a.min_mem_slack_gb}gb_corr-{a.min_corr}-{a.min_corr_tasks}tasks_other-{a.min_other}_inputsizes-{'yes' if input_sizes is not None else 'no'}")
    path = a.out / f"candidates__{data.stem.removeprefix('tasks__')}__{settings}.csv"
    out.to_csv(path)
    flags = ["multi_pipeline", "failures", "runtime", "inefficient", "input_sensitive"]
    print(f"{int(out['candidate'].sum())} candidates of {len(out)} modules; modules meeting each criterion:")
    print(out[flags].sum().to_string())
    with pd.option_context("display.width", 220, "display.max_columns", 30):
        print(out[out["candidate"]].head(15)[["pipelines", "tasks", "failure_rate", "median_runtime_min", "total_runtime_h",
                                                "cpu_eff_median", "mem_eff_median", "corr_rchar_rss", "corr_input_rss", "criteria_met"]].round(2))
    print(f"\nwritten to {path}")


if __name__ == "__main__":
    main()
