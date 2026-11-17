#!/usr/bin/env python3
"""
Module metrics for selected pipelines: which of their modules waste resources, fail, run long, and scale with input
size. Reads a tasks__*.parquet (and input_sizes__*.parquet if present) written by scripts/megatest_waste.py / scripts/input_sizes.py;
no S3 access.

One row per pipeline and module (completed tasks with usage data), sorted by total runtime within each pipeline:
  tasks, runs, runtime_h, median_runtime_min   size of the module in this pipeline
  cpu_waste_core_h, mem_waste_gbh              totals of (requested - used) x runtime
  req_mem_gb_median, peak_rss_gb_p95, mem_slack_gb, cpu_eff_median   request against actual use
  failure_rate, retry_rate                     FAILED share / share of tasks with attempt > 1, of finished tasks
  corr_{rchar,input}_{rss,runtime,cpu}         Spearman correlation of peak memory / runtime / CPU time with rchar or the
                                               real input size (>= 10 tasks and >= 3 distinct values, else empty)
  input_gb_min / _max, input_distinct_sizes    range the input correlations rest on
  pipelines_total                              number of pipelines the module appears in (whole dataset)

Usage:
  uv run python scripts/pipeline_view.py rnaseq ampliseq scrnaseq
"""
import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd


def _corr(g, x, y):
    d = g[g[x] > 0]
    if len(d) < 10 or d[x].nunique() < 3:
        return np.nan
    return d[[x, y]].rank().corr().iloc[0, 1]  # Spearman = Pearson correlation of the ranks


def build(t, sizes, wanted):
    """Metrics table for the (lower-case) pipeline names in `wanted`; `sizes` is the input_sizes table or None."""
    warnings.simplefilter("ignore", RuntimeWarning)
    total = t.groupby("module")["pipeline"].nunique()
    finished = t[t["status"].isin(["COMPLETED", "CACHED", "FAILED"])]
    keys = [finished["pipeline"], finished["module"]]
    fail = (finished["status"] == "FAILED").groupby(keys).mean()
    retry = (finished["attempt"] > 1).groupby(keys).mean()

    ok = t[t["status"].isin(["COMPLETED", "CACHED"])].dropna(subset=["peak_rss", "%cpu", "realtime"]).copy()
    ok = ok[ok["pipeline"].str.lower().isin(wanted)]
    if sizes is not None:
        s = sizes[(sizes["n_sized"] == sizes["n_files"]) & (sizes["input_bytes"] > 0)].rename(columns={"sample": "tag"})
        ok = ok.merge(s[["pipeline", "run", "tag", "input_bytes"]], on=["pipeline", "run", "tag"], how="left")
    else:
        ok["input_bytes"] = np.nan
    h = ok["realtime"] / 3.6e6
    ok["runtime_h"], ok["rss_gb"], ok["cpu_time_h"] = h, ok["peak_rss"] / 1e9, h * ok["%cpu"] / 100
    ok["cpu_waste"] = (ok["cpus"] - ok["%cpu"] / 100).clip(lower=0) * h
    ok["mem_waste"] = (ok["memory"] - ok["peak_rss"]).clip(lower=0) / 1e9 * h

    rows = []
    for (p, m), g in ok.groupby(["pipeline", "module"]):
        req, used = g["memory"].median() / 1e9, g["rss_gb"].quantile(0.95)
        row = dict(
            pipeline=p, module=m, tasks=len(g), runs=g["run"].nunique(),
            runtime_h=g["runtime_h"].sum(), median_runtime_min=g["runtime_h"].median() * 60,
            cpu_waste_core_h=g["cpu_waste"].sum(), mem_waste_gbh=g["mem_waste"].sum(),
            req_mem_gb_median=req, peak_rss_gb_p95=used, mem_slack_gb=max(req - used, 0),
            cpu_eff_median=(g["%cpu"] / 100 / g["cpus"]).median(),
            failure_rate=fail.get((p, m), 0.0), retry_rate=retry.get((p, m), 0.0))
        for xcol, name in (("rchar", "rchar"), ("input_bytes", "input")):
            for ycol, yname in (("rss_gb", "rss"), ("runtime_h", "runtime"), ("cpu_time_h", "cpu")):
                row[f"corr_{name}_{yname}"] = _corr(g, xcol, ycol)
        row.update(input_gb_min=g["input_bytes"].min() / 1e9, input_gb_max=g["input_bytes"].max() / 1e9,
                   input_distinct_sizes=g["input_bytes"].nunique(), pipelines_total=total.get(m, np.nan))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["pipeline", "runtime_h"], ascending=[True, False])


def newest_data(out, data=None):
    data = data or max(out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        raise SystemExit(f"No tasks__*.parquet in {out}; run scripts/megatest_waste.py first")
    sizes_file = out / f"input_sizes__{data.stem.removeprefix('tasks__')}.parquet"
    return data, (pd.read_parquet(sizes_file) if sizes_file.exists() else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipelines", nargs="+")
    ap.add_argument("--data", type=Path, help="tasks parquet (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    a = ap.parse_args()
    data, sizes = newest_data(a.out, a.data)
    t = pd.read_parquet(data)
    wanted = [p.lower() for p in a.pipelines]
    if not t["pipeline"].str.lower().isin(wanted).any():
        raise SystemExit(f"None of {wanted} found in {data}")
    out = build(t, sizes, wanted)
    path = a.out / f"pipeline_modules__{data.stem.removeprefix('tasks__')}__{'-'.join(sorted(wanted))}.csv"
    out.to_csv(path, index=False)
    print(out.groupby("pipeline").agg(modules=("module", "size"), tasks=("tasks", "sum"), runtime_h=("runtime_h", "sum")).round(1))
    print(f"\nwritten to {path}")


if __name__ == "__main__":
    main()
