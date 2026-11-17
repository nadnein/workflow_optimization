#!/usr/bin/env python3
"""
Plot how a module's resource use scales with input size: peak memory, runtime and CPU time against the sample's
input size (or `rchar`), one task per dot, on log-log axes, with a power-law fit over all tasks.

x axis (--x):
  input   real size of the sample's input files, from scripts/input_sizes.py (default if that file exists)
  rchar   bytes the task read through read calls (available for nearly all tasks)
Only the 3 pipelines with the most tasks get a colour; all others are grey ("other pipelines"). Panel titles show the
average per-pipeline Spearman correlation (rho, same definition as scripts/module_candidates.py) and the fitted exponent b of
y = a * x^b (b near 0: flat, near 1: proportional). Also writes the plotted points as CSV (table view).

Usage:
  uv run python scripts/plot_scaling.py PICARD_MARKDUPLICATES TRIMGALORE FASTQC
  uv run python scripts/plot_scaling.py TRIMGALORE --x rchar
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# first three slots of the validated categorical palette (valid for scatter plots), then muted grey for the rest
COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]
OTHER, INK, INK2, GRID, AXIS, SURFACE = "#898781", "#0b0b0b", "#52514e", "#e1e0d9", "#c3c2b7", "#ffffff"
PANELS = [("peak_rss_gb", "Peak memory (GB)"), ("runtime_min", "Runtime (min)"), ("cpu_time_h", "CPU time (core-hours)")]


def spearman_by_pipeline(d, ycol, min_tasks=10):
    """Mean over pipelines of the Spearman correlation of x with y (NaN if no pipeline qualifies)."""
    vals = [g[["x", ycol]].rank().corr().iloc[0, 1] for _, g in d.groupby("pipeline")
            if len(g) >= min_tasks and g["x"].nunique() >= 3]
    return np.nanmean(vals) if vals else np.nan


def make_figure(module, d, xlabel):
    d = d.copy()
    counts = d["pipeline"].value_counts()
    top = list(counts.index[:3])
    d["group"] = d["pipeline"].where(d["pipeline"].isin(top), "other pipelines")
    fig = make_subplots(rows=1, cols=3, horizontal_spacing=0.08, subplot_titles=[label for _, label in PANELS])
    for col, (ycol, ylabel) in enumerate(PANELS, 1):
        dd = d[d[ycol] > 0]
        for group, color in list(zip(top, COLORS)) + [("other pipelines", OTHER)]:
            g = dd[dd["group"] == group]
            if g.empty:
                continue
            fig.add_trace(go.Scattergl(
                x=g["x"], y=g[ycol], mode="markers", name=f"{group} ({int((d['group'] == group).sum())} tasks)",
                legendgroup=group, showlegend=(col == 1),
                marker=dict(size=8, color=color, opacity=0.65, line=dict(width=1, color=SURFACE)),
                customdata=np.stack([g["pipeline"], g["tag"].astype(str), g["run_date"].astype(str),
                                     g["tool_versions"].astype(str)], axis=-1),
                hovertemplate=("<b>%{customdata[0]}</b> · %{customdata[1]}<br>run %{customdata[2]}<br>"
                               f"{ylabel}: %{{y:.3g}}<br>{xlabel}: %{{x:.3g}}<br>%{{customdata[3]}}<extra></extra>"),
            ), row=1, col=col)
        # power-law fit over all tasks of this panel: log10(y) = log10(a) + b * log10(x)
        if len(dd) >= 3 and dd["x"].nunique() >= 2:
            b, loga = np.polyfit(np.log10(dd["x"]), np.log10(dd[ycol]), 1)
            xs = np.array([dd["x"].min(), dd["x"].max()])
            fig.add_trace(go.Scatter(x=xs, y=10 ** loga * xs ** b, mode="lines", line=dict(color=INK, width=2),
                                     showlegend=False, hoverinfo="skip"), row=1, col=col)
            rho = spearman_by_pipeline(dd, ycol)
            title = f"{ylabel}<br><span style='font-size:12px;color:{INK2}'>rho {rho:.2f} · exponent b {b:.2f}</span>"
        else:
            title = f"{ylabel}<br><span style='font-size:12px;color:{INK2}'>too few distinct sizes</span>"
        fig.layout.annotations[col - 1].update(text=title, font=dict(size=14, color=INK))
        fig.update_xaxes(type="log", title_text=xlabel, row=1, col=col)
        fig.update_yaxes(type="log", row=1, col=col)
    fig.update_xaxes(gridcolor=GRID, linecolor=AXIS, zeroline=False, tickfont=dict(color=INK2), title_font=dict(color=INK2))
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, zeroline=False, tickfont=dict(color=INK2))
    fig.update_layout(
        title=dict(text=f"<b>{module}</b>: resource use vs input size  ·  {d['pipeline'].nunique()} pipelines, {len(d)} tasks",
                   font=dict(size=16, color=INK), x=0.01),
        plot_bgcolor=SURFACE, paper_bgcolor=SURFACE, font=dict(family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        legend=dict(orientation="h", y=-0.22, x=0, font=dict(color=INK2)), margin=dict(t=110, b=100, l=60, r=20),
        height=520, hovermode="closest")
    return fig


def load_points(data, out, x=None):
    """Completed tasks of a tasks parquet with the chosen x axis; returns (table, x label, x mode, tag)."""
    data = Path(data)
    out = Path(out)
    tag = data.stem.removeprefix("tasks__")
    sizes_file = out / f"input_sizes__{tag}.parquet"
    xmode = x or ("input" if sizes_file.exists() else "rchar")
    t = pd.read_parquet(data)
    t = t[t["status"].isin(["COMPLETED", "CACHED"])].dropna(subset=["peak_rss", "%cpu", "realtime"]).copy()
    t["peak_rss_gb"] = t["peak_rss"] / 1e9
    t["runtime_min"] = t["realtime"] / 6e4
    t["cpu_time_h"] = t["realtime"] / 3.6e6 * t["%cpu"] / 100
    if xmode == "input":
        if not sizes_file.exists():
            raise SystemExit(f"{sizes_file} not found; run scripts/input_sizes.py or use --x rchar")
        s = pd.read_parquet(sizes_file)
        s = s[(s["n_sized"] == s["n_files"]) & (s["input_bytes"] > 0)].rename(columns={"sample": "tag"})
        t = t.merge(s[["pipeline", "run", "tag", "input_bytes"]], on=["pipeline", "run", "tag"])
        t["x"], xlabel = t["input_bytes"] / 1e9, "Input size of the sample (GB)"
    else:
        t["x"], xlabel = t["rchar"] / 1e9, "Bytes read by the task, rchar (GB)"
    return t[t["x"] > 0], xlabel, xmode, tag


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("modules", nargs="+")
    ap.add_argument("--x", choices=["input", "rchar"], help="x axis (default: input if input_sizes exists, else rchar)")
    ap.add_argument("--data", type=Path, help="tasks parquet (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    a = ap.parse_args()
    data = a.data or max(a.out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        raise SystemExit(f"No tasks__*.parquet in {a.out}")
    t, xlabel, xmode, tag = load_points(data, a.out, a.x)

    plots = a.out / "plots"
    plots.mkdir(exist_ok=True)
    for module in (m.upper() for m in a.modules):
        d = t[t["module"] == module]
        if d.empty:
            print(f"{module}: no tasks with {xmode} data, skipped")
            continue
        fig = make_figure(module, d, xlabel)
        base = plots / f"scaling__{tag}__{xmode}__{module}"
        fig.write_html(f"{base}.html", include_plotlyjs=True)
        d[["pipeline", "run", "run_date", "tag", "tool_versions", "x", "peak_rss_gb", "runtime_min", "cpu_time_h"]] \
            .rename(columns={"x": f"x_{xmode}_gb", "tag": "sample"}).to_csv(f"{base}.csv", index=False)
        print(f"{module}: {len(d)} tasks, {d['pipeline'].nunique()} pipelines, x = {xmode}; written to {base}.html (+ .csv)")


if __name__ == "__main__":
    main()
