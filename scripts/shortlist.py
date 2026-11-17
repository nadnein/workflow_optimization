#!/usr/bin/env python3
"""
Shortlist of modules worth optimising for the pipelines you actually use, from the AWS test data plus the research notes.

Applies four criteria to every module of the chosen pipelines (metrics from scripts/pipeline_view.py, all within these pipelines):
  1. failures         failure_rate or retry_rate >= --min-fail-rate
  2. runtime          median task runtime >= --min-median-runtime-min, or module runtime >= --min-runtime-h hours in the
                      pipeline, or >= --many-tasks tasks
  3. inefficient      median CPU efficiency <= --max-cpu-eff, or memory slack (median request minus 95th percentile of
                      peak use) >= --min-mem-slack-gb
  4. input_dependent  any Spearman correlation of peak memory / runtime / CPU time with rchar or real input size
                      >= --min-corr
No module is dropped for having little data: modules with fewer than --min-tasks tasks stay in the list and are marked
`low data` in the `evidence` column (their rates and correlations are shaky), and are ordered after better-supported
modules with the same number of criteria. --min-module-runtime-h (default 0) can hide trivial modules. Rows are ordered
by the number of criteria met, then evidence, then runtime.
The research notes (docs/tool_scaling_evidence.csv) are joined where the module is covered. `next_step` is a rule of
thumb (see docs/workflow.md), not a verdict. Writes shortlist__<tag>__<pipelines>.csv. No S3 access.

Usage:
  uv run python scripts/shortlist.py rnaseq ampliseq scrnaseq
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline_view import build, newest_data

CORR_COLS = [f"corr_{x}_{y}" for x in ("rchar", "input") for y in ("rss", "runtime", "cpu")]


def next_step(r, a):
    """Rule-of-thumb suggestion from the flags (see docs/workflow.md, step 4)."""
    steps = []
    if r["failures"]:
        steps.append("check why tasks fail (out of memory, walltime?) before changing requests")
    if r["input_dependent"]:
        narrow = r["input_distinct_sizes"] < a.min_sizes or not r["input_gb_max"] >= a.min_range * r["input_gb_min"]
        steps.append("run an experiment with inputs over a wider size range" if narrow
                     else "fit a request formula (input range is already wide)")
    elif r["inefficient"]:
        steps.append("no input dependence found: try a smaller fixed request, validated on more data")
    if not steps:
        steps.append("low priority")
    return "; ".join(steps)


def reasons(r):
    out = []
    if r["failures"]:
        out.append(f"failures (failed {r['failure_rate']:.0%}, retried {r['retry_rate']:.0%})")
    if r["runtime"]:
        out.append(f"runtime ({r['runtime_h']:.1f} h in {r['tasks']} tasks, median {r['median_runtime_min']:.1f} min)")
    if r["inefficient"]:
        out.append(f"inefficient (cpu efficiency {r['cpu_eff_median']:.2f}, {r['mem_slack_gb']:.0f} GB memory slack)")
    if r["input_dependent"]:
        best = r[CORR_COLS].astype(float).idxmax()
        out.append(f"input-dependent ({best.removeprefix('corr_')} rho {r[best]:.2f})")
    return "; ".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipelines", nargs="+")
    ap.add_argument("--data", type=Path, help="tasks parquet (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    ap.add_argument("--min-tasks", type=int, default=10, help="below this a module is marked `low data` (not removed)")
    ap.add_argument("--min-module-runtime-h", type=float, default=0.0, help="hide modules with less runtime in the pipeline (default: hide none)")
    ap.add_argument("--min-fail-rate", type=float, default=0.02)
    ap.add_argument("--min-median-runtime-min", type=float, default=10)
    ap.add_argument("--min-runtime-h", type=float, default=10)
    ap.add_argument("--many-tasks", type=int, default=500)
    ap.add_argument("--max-cpu-eff", type=float, default=0.3)
    ap.add_argument("--min-mem-slack-gb", type=float, default=16)
    ap.add_argument("--min-corr", type=float, default=0.5)
    ap.add_argument("--min-sizes", type=int, default=10, help="fewer distinct input sizes than this count as a narrow range")
    ap.add_argument("--min-range", type=float, default=10, help="input max/min below this counts as a narrow range")
    ap.add_argument("--evidence", default=Path(__file__).parent.parent / "docs" / "tool_scaling_evidence.csv", type=Path)
    a = ap.parse_args()

    data, sizes = newest_data(a.out, a.data)
    wanted = [p.lower() for p in a.pipelines]
    t = pd.read_parquet(data)
    if not t["pipeline"].str.lower().isin(wanted).any():
        raise SystemExit(f"None of {wanted} found in {data}")
    d = build(t, sizes, wanted)
    d = d[d["runtime_h"] >= a.min_module_runtime_h].copy()
    d["evidence"] = np.where(d["tasks"] >= a.min_tasks, "ok", "low data")

    d["failures"] = (d["failure_rate"] >= a.min_fail_rate) | (d["retry_rate"] >= a.min_fail_rate)
    d["runtime"] = ((d["median_runtime_min"] >= a.min_median_runtime_min) | (d["runtime_h"] >= a.min_runtime_h)
                    | (d["tasks"] >= a.many_tasks))
    d["inefficient"] = (d["cpu_eff_median"] <= a.max_cpu_eff) | (d["mem_slack_gb"] >= a.min_mem_slack_gb)
    d["input_dependent"] = d[CORR_COLS].max(axis=1) >= a.min_corr
    flags = ["failures", "runtime", "inefficient", "input_dependent"]
    d["criteria_met"] = d[flags].sum(axis=1)
    d["reasons"] = d.apply(reasons, axis=1)
    d["next_step"] = d.apply(lambda r: next_step(r, a), axis=1)

    ev = pd.read_csv(a.evidence).drop_duplicates("module").set_index("module")
    d = d.join(ev[["tool", "documented_memory_driver", "documented_runtime_driver", "evidence"]].add_prefix("research_"), on="module")
    d["_ok"] = d["evidence"] == "ok"
    d = d.sort_values(["criteria_met", "_ok", "runtime_h"], ascending=False).drop(columns="_ok")

    tag = data.stem.removeprefix("tasks__")
    path = a.out / f"shortlist__{tag}__{'-'.join(sorted(wanted))}.csv"
    first = ["pipeline", "module", "criteria_met", "evidence", "reasons", "next_step"] + flags
    d[first + [c for c in d.columns if c not in first]].to_csv(path, index=False)

    print(f"{len(d)} modules listed, {int((d['evidence'] == 'low data').sum())} of them with low data (< {a.min_tasks} tasks)")
    print(d.groupby("pipeline")["criteria_met"].value_counts().unstack(fill_value=0).sort_index(axis=1, ascending=False))
    print(f"\nwritten to {path}")
    with pd.option_context("display.width", 250, "display.max_colwidth", 70):
        for p, g in d.groupby("pipeline"):
            print(f"\n== {p}: top modules")
            print(g.head(7)[["module", "criteria_met", "evidence", "reasons"]].to_string(index=False))


if __name__ == "__main__":
    main()
