#!/usr/bin/env python3
"""
Turn Nextflow trace files from our own cluster into the per-task table that the AWS analysis uses, so that
scripts/pipeline_view.py, scripts/shortlist.py, scripts/module_detail.py and scripts/plot_scaling.py work on our runs as well. Everything runs locally;
only the reduced table (no node names, hashes or paths) is written.

Input: data/<folder>/*.txt, where the folder name contains the pipeline name (case-insensitive substring, e.g. data/rnaseq/).
The traces must be raw (`trace.raw = true` in nextflow.config: milliseconds and bytes, no units) and should contain
process, name, status, exit, attempt, cpus, memory, time, realtime, %cpu, peak_rss and ideally rchar. Failed tasks are
kept (the old notebook loader dropped them). The sample is taken from the `tag` column or, if missing, from the task name
`PROCESS (sample)`.

Writes <out>/tasks__own.parquet with the same columns as the AWS table (container and tool_versions stay empty/unknown).

Usage:
  uv run python scripts/own_traces.py rnaseq ampliseq scrnaseq
"""
import argparse
import re
from pathlib import Path

import pandas as pd

NUMERIC = ["exit", "attempt", "cpus", "memory", "time", "realtime", "%cpu", "peak_rss", "read_bytes", "write_bytes",
           "rchar", "wchar"]
KEEP = ["process", "status", "exit", "attempt", "cpus", "memory", "time", "realtime", "%cpu", "peak_rss", "read_bytes",
        "write_bytes", "rchar", "wchar", "container", "tag"]


def find_trace_files(pipeline, data_dir="data"):
    """All *.txt files in the data_dir subfolders whose name contains the pipeline name."""
    data_dir = Path(data_dir)
    folders = sorted(d for d in data_dir.iterdir() if d.is_dir() and pipeline.lower() in d.name.lower())
    return [f for d in folders for f in sorted(d.glob("*.txt"))]


def load_trace(path, pipeline):
    """One trace file as a per-task table in the AWS layout."""
    df = pd.read_csv(path, sep="\t", engine="python", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    if "peak_rss" in df and not df["peak_rss"].fillna("-").str.fullmatch(r"[\d,.\-]+|nan").all():
        raise ValueError(f"{path.name}: values with units (e.g. '1.2 GB'); rerun Nextflow with `trace.raw = true`")
    for c in NUMERIC:
        if c in df:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False).str.strip().replace("-", "nan"),
                                  errors="coerce")
    if "attempt" not in df:
        df["attempt"] = 1
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    tag = df["tag"].where(df["tag"].fillna("-") != "-") if "tag" in df else pd.Series(index=df.index, dtype=object)
    if "name" in df:
        tag = tag.fillna(df["name"].str.extract(r"\((.*)\)\s*$")[0])
    df["tag"] = tag
    df = df.reindex(columns=KEEP)  # columns missing in the trace (e.g. container) become empty
    date = re.search(r"(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}", path.stem)
    df["pipeline"] = pipeline.lower()
    df["run"] = path.stem
    df["run_date"] = date.group(1) if date else None
    df["pipeline_version"] = None
    df["module"] = df["process"].astype(str).str.split(":").str[-1]
    df["tool_versions"] = "unknown"
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipelines", nargs="+")
    ap.add_argument("--data-dir", default="data", type=Path)
    ap.add_argument("--out", default="own_summary", type=Path)
    a = ap.parse_args()
    frames = []
    for p in a.pipelines:
        files = find_trace_files(p, a.data_dir)
        if not files:
            print(f"{p}: no trace files in {a.data_dir}/*{p}*/ (skipped)")
            continue
        for f in files:
            df = load_trace(f, p)
            usable = df[df["status"].isin(["COMPLETED", "CACHED"])].dropna(subset=["peak_rss", "%cpu", "realtime"])
            print(f"{p}: {f.name}: {len(df)} tasks, {len(usable)} with usage data, "
                  f"{int((df['status'] == 'FAILED').sum())} failed, {df['module'].nunique()} modules"
                  f"{'' if df['rchar'].notna().any() else ', no rchar'}")
            frames.append(df)
    if not frames:
        raise SystemExit("No trace files found")
    a.out.mkdir(exist_ok=True)
    path = a.out / "tasks__own.parquet"
    pd.concat(frames, ignore_index=True).to_parquet(path)
    print(f"\nwritten to {path}")


if __name__ == "__main__":
    main()
