#!/usr/bin/env python3
"""
Detailed view of selected modules from the per-task table written by scripts/megatest_waste.py.
One row per module, pipeline, run and tool-version combination, so you can see which
pipelines used a module, with which versions and when. Reads a tasks__*.parquet only (no S3 access); by default the newest one in --out.

Usage:
  uv run python scripts/module_detail.py TRIMGALORE FASTQC
  uv run python scripts/module_detail.py TRIMGALORE --data megatest_summary/tasks__all__since-2023-01-01.parquet
"""
import argparse
from pathlib import Path

import pandas as pd

from megatest_waste import summarise

KEYS = ["module", "pipeline", "pipeline_version", "run", "run_date", "tool_versions"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("modules", nargs="+", help="module names, e.g. TRIMGALORE (case-insensitive)")
    ap.add_argument("--data", type=Path, help="tasks parquet to use (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    a = ap.parse_args()

    data = a.data or max(a.out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        raise SystemExit(f"No tasks__*.parquet in {a.out}; run scripts/megatest_waste.py first")
    df = pd.read_parquet(data)
    wanted = [m.upper() for m in a.modules]
    df = df[df["module"].str.upper().isin(wanted)]
    if df.empty:
        raise SystemExit(f"No tasks found for {', '.join(wanted)} in {data}")
    # missing values (e.g. runs without a version file or timestamp) must not drop rows from the groupby
    df = df.assign(**{k: df[k].fillna("unknown") for k in KEYS[1:]})
    detail = summarise(df, KEYS).drop(columns=["pipelines", "runs", "versions", "latest_run", "ranked"])
    detail = detail.sort_index(level=["module", "pipeline", "run_date"])
    tag = data.stem.removeprefix("tasks__")  # carry over the settings of the data the detail is based on
    path = a.out / f"detail__{tag}__{'-'.join(wanted)}.csv"
    detail.to_csv(path)
    with pd.option_context("display.width", 250, "display.max_columns", 30, "display.max_colwidth", 45):
        print(detail.round(2).to_string())
    print(f"\nwritten to {path}")


if __name__ == "__main__":
    main()
