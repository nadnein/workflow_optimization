#!/usr/bin/env python3
"""
Is `rchar` (bytes a task read through read calls) a good proxy for the real input size of its sample?

Joins the tasks of a tasks__*.parquet to the per-sample input sizes of scripts/input_sizes.py (task tag == sample) and, per
module, reports how well rchar follows the input size:
  corr_rchar_input   Spearman correlation of rchar with input size, computed per pipeline (>= --min-tasks tasks and at
                     least 3 distinct input sizes) and averaged over pipelines
  median_ratio       median of rchar / input size (about 1: reads the input once; much larger: re-reads data or reads
                     large references or indexes)
Only COMPLETED/CACHED tasks and fully sized samples are used. Writes rchar_vs_input__<tag>.csv (one row per module)
and rchar_vs_input_by_pipeline__<tag>.csv (one row per module and pipeline, with the 10th/90th percentile of the ratio
and the median input size and rchar). The pooled ratio can hide very different behaviour per pipeline.

Usage:
  uv run python scripts/rchar_vs_input.py
"""
import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, help="tasks parquet (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    ap.add_argument("--min-tasks", type=int, default=10)
    a = ap.parse_args()
    data = a.data or max(a.out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        raise SystemExit(f"No tasks__*.parquet in {a.out}")
    tag = data.stem.removeprefix("tasks__")
    sizes_file = a.out / f"input_sizes__{tag}.parquet"
    if not sizes_file.exists():
        raise SystemExit(f"{sizes_file} not found; run scripts/input_sizes.py first")

    sizes = pd.read_parquet(sizes_file)
    sizes = sizes[(sizes["n_sized"] == sizes["n_files"]) & (sizes["input_bytes"] > 0)].rename(columns={"sample": "tag"})
    t = pd.read_parquet(data)
    t = t[t["status"].isin(["COMPLETED", "CACHED"]) & (t["rchar"] > 0)]
    t = t.merge(sizes[["pipeline", "run", "tag", "input_bytes"]], on=["pipeline", "run", "tag"])
    t["ratio"] = t["rchar"] / t["input_bytes"]
    print(f"{len(t)} tasks from {t[['pipeline', 'run']].drop_duplicates().shape[0]} runs have both rchar and a real input size")

    warnings.simplefilter("ignore", RuntimeWarning)
    corr = {}
    for (module, _), g in t.groupby(["module", "pipeline"]):
        if len(g) >= a.min_tasks and g["input_bytes"].nunique() >= 3:
            r = g[["rchar", "input_bytes"]].rank()
            corr.setdefault(module, []).append(r["rchar"].corr(r["input_bytes"]))
    out = pd.DataFrame({
        "pipelines": t.groupby("module")["pipeline"].nunique(),
        "tasks": t.groupby("module").size(),
        "median_ratio": t.groupby("module")["ratio"].median(),
        "corr_rchar_input": pd.Series({m: np.nanmean(v) for m, v in corr.items()}),
    })
    out = out[out["tasks"] >= a.min_tasks].sort_values("corr_rchar_input", ascending=False)
    by_pipe = t.groupby(["module", "pipeline"]).agg(
        tasks=("ratio", "size"),
        median_ratio=("ratio", "median"),
        ratio_p10=("ratio", lambda v: v.quantile(0.1)),
        ratio_p90=("ratio", lambda v: v.quantile(0.9)),
        median_input_gb=("input_bytes", lambda v: v.median() / 1e9),
        median_rchar_gb=("rchar", lambda v: v.median() / 1e9),
        distinct_input_sizes=("input_bytes", "nunique"),
    )
    by_pipe["corr_rchar_input"] = pd.Series({(m, pl): g[["rchar", "input_bytes"]].rank().corr().iloc[0, 1]
                                             for (m, pl), g in t.groupby(["module", "pipeline"])
                                             if len(g) >= a.min_tasks and g["input_bytes"].nunique() >= 3})
    by_pipe = by_pipe[by_pipe["tasks"] >= a.min_tasks]
    pipe_path = a.out / f"rchar_vs_input_by_pipeline__{tag}.csv"
    by_pipe.to_csv(pipe_path)
    path = a.out / f"rchar_vs_input__{tag}.csv"
    out.to_csv(path)
    with pd.option_context("display.width", 160, "display.max_rows", 60):
        print(out.round(2).head(30))
    ok = out["corr_rchar_input"].dropna()
    print(f"\n{len(ok)} modules with a correlation; median {ok.median():.2f}; "
          f"{(ok >= 0.8).sum()} with >= 0.8; written to {path} and {pipe_path}")


if __name__ == "__main__":
    main()
