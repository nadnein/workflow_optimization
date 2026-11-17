
"""
Nextflow trace analysis helper functions.
 
This module provides utilities for loading, preprocessing, and analyzing
Nextflow trace files to identify resource optimization opportunities and
compare resource usage across pipeline runs.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ══════════════════════════════════════════════════════════════════════════════
# LABEL GENERATION
# ══════════════════════════════════════════════════════════════════════════════
 
def make_unique_labels(names, separator=":"):
    """Return the shortest unique suffix label for each name.

    Nextflow process names are colon-separated paths, e.g.
    "PIPELINE:SUBWORKFLOW:PROCESS". This function trims each name to the
    shortest trailing suffix (rightmost component first) that makes every
    label in the collection distinct.

    Example:
        ["WF:TRIM", "WF:ALIGN", "WF:ALIGN"] →  ["TRIM", "ALIGN", "ALIGN"]
        but if two processes share the last component, extend to two parts:
        ["WF:A:ALIGN", "WF:B:ALIGN"] → ["A:ALIGN", "B:ALIGN"]
    """
    # Remember whether the caller passed an Index so we return the same type
    input_is_index = isinstance(names, pd.Index)
    names_series = pd.Series(names, dtype="string")

    # Split each name into its path components, e.g. ["WF", "SUB", "PROC"]
    parts = names_series.str.split(separator)
    max_depth = int(parts.str.len().max())  # maximum number of components in any name

    # Start with just the last component (depth = 1)
    labels = parts.str[-1].copy()

    # Iteratively include one more component from the right for any labels
    # that are still duplicated, until all labels are unique or we've used
    # every component available
    for depth in range(2, max_depth + 1):
        dup_mask = labels.duplicated(keep=False)
        if not dup_mask.any():
            break  # all labels are now unique — stop early
        # For duplicated entries, rebuild the label using `depth` trailing parts
        labels.loc[dup_mask] = parts.loc[dup_mask].apply(
            lambda x: (
                separator.join(x[-depth:]) if len(x) >= depth else separator.join(x)
            )
        )

    if input_is_index:
        return pd.Index(labels)
    return labels


def truncate_left(text, max_len=40, prefix="..."):
    """Left-truncate a string so the rightmost characters are always visible.

    When a label is too long to display cleanly (e.g. in a chart axis), this
    removes characters from the left rather than the right. That way the most
    specific part of a Nextflow process name — which is always at the end —
    is preserved.

    Example:
        "VERY:LONG:PIPELINE:PROCESS_NAME" (32 chars, max_len=20)
        → "...PROCESS_NAME"  (keeps the last 17 chars + "..." prefix)
    """
    text = str(text)
    # Nothing to do if the text already fits within the limit
    if len(text) <= max_len:
        return text
    # Calculate how many characters from the right we can keep after the prefix
    keep = max_len - len(prefix)
    return prefix + text[-keep:]


def _print_consistency_summary(
    process_df: pd.DataFrame,
    filenames: List[str],
    labels: List[str],
    consistent_processes: List[str],
) -> None:
    """Print a concise summary of which processes are shared across runs."""
    total_unique = process_df["process"].nunique()
    consistent_set = set(consistent_processes)
    shared_pct = (len(consistent_set) / total_unique * 100.0) if total_unique else 0.0

    print("Consistency")
    print(
        f"  Shared: {len(consistent_set)}/{total_unique} process(es) "
        f"across {len(labels)} run(s) ({shared_pct:.1f}%)"
    )

    exclusive_counts = [
        len(set(process_df.loc[process_df["filename"] == f, "process"]) - consistent_set)
        for f in filenames
    ]
    if sum(exclusive_counts) == 0:
        print("  Run-specific: none")
    else:
        print(
            "  Run-specific: "
            + ", ".join(f"{label} {n}" for label, n in zip(labels, exclusive_counts))
        )
    print()


def make_display_labels(names, separator=":", max_len=40, prefix="..."):
    """Produce chart-ready labels: unique suffix first, then truncated to fit.

    Combines make_unique_labels and truncate_left into a single convenience
    function. The two-step approach means we shorten from the left only after
    uniqueness has already been ensured, so the distinguishing suffix is never
    cut off.
    """
    # Step 1: shorten each name to its shortest unique suffix
    unique = make_unique_labels(names, separator=separator)

    # Remember whether the caller passed an Index so we return the same type
    input_is_index = isinstance(unique, pd.Index)
    unique_series = pd.Series(unique, dtype="string")

    # Step 2: left-truncate any labels that are still too long to display
    display_labels = unique_series.apply(
        lambda s: truncate_left(s, max_len=max_len, prefix=prefix)
    )

    if input_is_index:
        return pd.Index(display_labels)
    return display_labels


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING & PREPROCESSING
# ══════════════════════════════════════════════════════════════════════════════
 
def load_and_preprocess_trace(trace_path, verbose=True):
    """Load a Nextflow trace file and return a clean, analysis-ready DataFrame.

    Applies three cleaning steps in order:
      1. Status filter  — keeps only COMPLETED and CACHED tasks; failed or
                          running tasks are set aside and reported.
      2. Numeric cleaning — Nextflow writes values like "87.3%", "1,024", or
                            "-" for missing fields. These are normalised to
                            plain floats (invalid entries become NaN).
      3. Validity filter — drops rows where any of the five essential metrics
                           (%cpu, cpus, peak_rss, memory, realtime) are NaN
                           or where cpus ≤ 0, since those rows cannot be used
                           in any downstream calculation.

    With verbose=True a short summary is printed so you can quickly spot
    whether a trace file has unexpected data quality issues.
    """
    trace_path = Path(trace_path)

    # Read the tab-separated trace file; strip whitespace from column names
    # (Nextflow occasionally adds leading/trailing spaces)
    df = pd.read_csv(trace_path, sep="\t", engine="python")
    df.columns = [str(c).strip() for c in df.columns]

    initial_count = len(df)

    # ── Step 1: Status filter ─────────────────────────────────────────────────
    excluded_by_status = pd.DataFrame()
    if "status" in df.columns:
        status_norm = df["status"].astype(str).str.strip().str.upper()
        completed_mask = status_norm.isin(["COMPLETED", "CACHED"])
        excluded_by_status = df[~completed_mask].copy()  # keep for reporting
        df = df[completed_mask].copy()

    # ── Step 2: Numeric cleaning ──────────────────────────────────────────────
    numeric_cols = [
        "%cpu",        # CPU utilisation as a percentage
        "cpus",        # number of CPUs requested
        "peak_rss",    # peak resident memory (bytes)
        "memory",      # memory requested (bytes)
        "realtime",    # wall-clock runtime (milliseconds)
        "read_bytes",  # bytes read from disk
        "write_bytes", # bytes written to disk
        "rchar",       # characters read (alternative I/O metric)
        "wchar",       # characters written (alternative I/O metric)
        "submit",      # task submission timestamp
        "start",       # task start timestamp
        "complete",    # task completion timestamp
        "duration",    # total duration (ms)
        "attempt",     # attempt number (for retries)
        "time"         # requested runtime (ms) 
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)    # remove thousands separators
                .str.replace("-", "nan", regex=False) # Nextflow uses "-" for missing
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Step 3: Validity filter ───────────────────────────────────────────────
    # A task is unusable if any essential metric is missing or cpus is zero/negative
    excluded_mask = df[["%cpu", "cpus", "peak_rss", "memory", "realtime", "time"]].isna().any(
        axis=1
    ) | (df["cpus"] <= 0)
    excluded_numeric = df[excluded_mask].copy()  # keep for reporting
    df = df[~excluded_mask].copy()

    excluded_tasks = pd.concat([excluded_by_status, excluded_numeric], axis=0)

    # ── Optional summary ─────────────────────────────────────────────────────
    if verbose:
        print(f"Total tasks in trace: {initial_count}")
        print(f"Loaded {len(df)} tasks for analysis")

        if len(excluded_by_status) > 0:
            print(f"Excluded non-COMPLETED/CACHED tasks: {len(excluded_by_status)}")
            cols = [c for c in ["name", "status", "attempt"] if c in excluded_by_status.columns]
            if cols:
                print(excluded_by_status[cols].value_counts().to_string())
            print()

        if len(excluded_numeric) > 0:
            print(
                "Excluded COMPLETED/CACHED tasks with missing/invalid numeric "
                f"metrics: {len(excluded_numeric)}"
            )
            cols = [c for c in ["name", "status"] if c in excluded_numeric.columns]
            if cols:
                print(excluded_numeric[cols].value_counts().to_string())
            print()

        print(f"Excluded total: {len(excluded_tasks)}")

    return df

def find_trace_files(pipeline: str, data_dir: Path | str = "data") -> List[Path]:
    """Return all trace files in ``data_dir`` subfolders whose name contains ``pipeline``.

    Folder matching is a case-insensitive substring match, so ``"rnaseq"``
    picks up ``data/rnaseq`` as well as e.g. ``data/rnaseq_ramses``.
    """
    data_dir = Path(data_dir)
    folders = sorted(
        d for d in data_dir.iterdir()
        if d.is_dir() and pipeline.lower() in d.name.lower()
    )
    if not folders:
        raise FileNotFoundError(
            f"No folder containing '{pipeline}' found in {data_dir.resolve()}"
        )
    trace_files = [f for d in folders for f in sorted(d.glob("*.txt"))]
    if not trace_files:
        raise FileNotFoundError(
            f"No .txt trace files in: {', '.join(str(d) for d in folders)}"
        )
    return trace_files


def _run_label(trace_file: Path) -> str:
    """Readable run label: file stem without the trailing timestamp."""
    stem = re.sub(r"_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$", "", trace_file.stem)
    return stem.replace("_", " ")


def load_pipeline_traces(
    pipeline: str, data_dir: Path | str = "data"
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Path], List[str]]:
    """Load and process every trace file of a pipeline.

    Trace files are discovered with :func:`find_trace_files`: all ``*.txt``
    files in the ``data_dir`` subfolders whose name contains ``pipeline``.

    Returns:
        Tuple of (run_summary_df, process_df, trace_files, labels)
    """
    trace_files = find_trace_files(pipeline, data_dir)
    labels = [_run_label(f) for f in trace_files]
    if len(set(labels)) != len(labels):  # fall back to full stems if ambiguous
        labels = [f.stem for f in trace_files]

    print(f"Pipeline: {pipeline} | {len(trace_files)} run(s)")

    run_results = []
    process_records = []
    for trace_file, label in zip(trace_files, labels):
        df_trace = load_and_preprocess_trace(trace_file, verbose=False)
        run_metrics = build_run_summary(df_trace)
        run_metrics["filename"] = trace_file.stem
        run_metrics["label"] = label
        run_results.append(run_metrics)

        print(
            f"  - {label}"
            f" | {len(df_trace)} task(s)"
            f" | {run_metrics['process_count']} process(es)"
            f" | {trace_file.parent.name}/{trace_file.name}"
        )

        pp = build_per_process_summary(df_trace)
        pp["filename"] = trace_file.stem
        pp["label"] = label
        pp["process_display"] = make_display_labels(pp["process"], max_len=20)
        pp["process_name"] = make_unique_labels(pp["process"])
        process_records.append(pp)
    print()

    run_summary_df = pd.DataFrame(run_results)
    process_df = pd.concat(process_records, ignore_index=True)
    process_df = annotate_consistent_processes(process_df)

    print("Summary")
    print(
        f"  {len(run_summary_df)} run(s)"
        f" | {process_df['process'].nunique()} unique process(es)"
    )

    return run_summary_df, process_df, trace_files, labels

# ══════════════════════════════════════════════════════════════════════════════
# RESOURCE CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════════
 
def _get_io_bytes_series(df: pd.DataFrame) -> pd.Series:
    """Return the best available per-task I/O byte series."""
    if "read_bytes" in df.columns and "write_bytes" in df.columns:
        return df["read_bytes"].fillna(0) + df["write_bytes"].fillna(0)
    if "rchar" in df.columns and "wchar" in df.columns:
        return df["rchar"].fillna(0) + df["wchar"].fillna(0)
    return pd.Series(0, index=df.index, dtype=float)


def add_runtime_and_resource_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add core runtime, CPU, memory, and I/O usage columns used by summaries."""
    work = df.copy()
    work["runtime_hours"] = work["realtime"] / 3_600_000.0
    work["cpu_used_cores"] = work["%cpu"] / 100.0
    work["mem_used_gb"] = work["peak_rss"] / (1024**3)
    work["io_bytes"] = _get_io_bytes_series(work)
    work["io_gb"] = work["io_bytes"] / (1024**3)
    return work


def add_resource_contribution_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add resource contribution columns derived from runtime and usage."""
    work = add_runtime_and_resource_columns(df)
    work["cpu_h"] = work["cpu_used_cores"] * work["runtime_hours"]
    work["memory_h"] = work["mem_used_gb"] * work["runtime_hours"]
    return work


def compute_run_general_summary(df: pd.DataFrame) -> Dict[str, float]:
    """Compute run-level counts and overall runtime."""
    return {
        "task_count": len(df),
        "process_count": df["process"].nunique(),
        "total_runtime_h": df["realtime"].sum() / 3_600_000.0,
    }


def compute_run_resource_summary(df: pd.DataFrame) -> Dict[str, float]:
    """Compute run-level CPU, memory, and I/O contribution totals."""
    work = add_resource_contribution_columns(df)
    return {
        "cpu_h": work["cpu_h"].sum(),
        "memory_h": work["memory_h"].sum(),
        "io_gb": work["io_gb"].sum(),
        "total_io_bytes": work["io_bytes"].sum(),
    }


def build_run_summary(df: pd.DataFrame) -> Dict[str, float]:
    """Build one run-summary row from general and resource summary columns."""
    return {
        **compute_run_general_summary(df),
        **compute_run_resource_summary(df),
    }


def compute_per_process_general_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-process counts used across multiple summaries."""
    return (
        df.groupby("process")
        .agg(
            n_tasks=("process", "size"),
            total_runtime_hours=("realtime", lambda s: s.sum() / 3_600_000.0),
        )
        .reset_index()
    )


def compute_per_process_usage_metrics(work: pd.DataFrame) -> pd.DataFrame:
    """Compute per-process total resource burdens."""
    return (
        work.groupby("process")
        .agg(
            cpu_h=("cpu_h", "sum"),
            memory_h=("memory_h", "sum"),
            io_gb=("io_gb", "sum"),
        )
        .reset_index()
    )


def add_efficiency_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add requested-vs-used resource columns and efficiency metrics."""
    work = add_resource_contribution_columns(df)
    work["mem_requested_gb"] = work["memory"] / (1024**3)
    work["time_requested_hours"] = work["time"] / 3_600_000.0
    work["task_cpu_efficiency"] = (work["cpu_used_cores"] / work["cpus"]) * 100.0
    work["task_mem_efficiency"] = (work["mem_used_gb"] / work["mem_requested_gb"]) * 100.0
    work["task_time_efficiency"] = (
        work["runtime_hours"] / work["time_requested_hours"]
    ) * 100.0
    work["cpu_requested_h"] = work["cpus"] * work["runtime_hours"]
    work["memory_requested_h"] = work["mem_requested_gb"] * work["runtime_hours"]
    return work


def _report_efficiency_exceedances(work: pd.DataFrame) -> None:
    """Print warnings for tasks that exceeded requested resources."""
    cpu_exceeded = work["task_cpu_efficiency"] > 100.0
    mem_exceeded = work["task_mem_efficiency"] > 100.0
    time_exceeded = work["task_time_efficiency"] > 100.0

    if cpu_exceeded.any():
        n_exceed = cpu_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks used more CPU cores than requested!")
        print("   Scheduler may not be enforcing CPU limits properly.")
        exceed_details = work[cpu_exceeded][
            ["process", "cpus", "cpu_used_cores", "task_cpu_efficiency"]
        ].head(5)
        print("   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(
                f"     {row['process']}: requested {row['cpus']:.1f} cores, "
                f"used {row['cpu_used_cores']:.1f} ({row['task_cpu_efficiency']:.1f}%)"
            )
        print()

    if mem_exceeded.any():
        n_exceed = mem_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks used more memory than requested!")
        print("   Memory limits may not be enforced properly.")
        exceed_details = work[mem_exceeded][
            ["process", "mem_requested_gb", "mem_used_gb", "task_mem_efficiency"]
        ].head(5)
        print("   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(
                f"     {row['process']}: requested {row['mem_requested_gb']:.2f} GB, "
                f"used {row['mem_used_gb']:.2f} GB ({row['task_mem_efficiency']:.1f}%)"
            )
        print()

    if time_exceeded.any():
        n_exceed = time_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks ran longer than requested time!")
        print("   Time limits may not be enforced, or limits are set too tight.")
        exceed_details = work[time_exceeded][
            ["process", "time_requested_hours", "runtime_hours", "task_time_efficiency"]
        ].head(5)
        print("   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(
                f"     {row['process']}: requested {row['time_requested_hours']:.2f} h, "
                f"took {row['runtime_hours']:.2f} h ({row['task_time_efficiency']:.1f}%)"
            )
        print()


def compute_per_process_efficiency_metrics(work: pd.DataFrame, verbose: bool = True):
    """Compute per-process weighted efficiencies.

    For each task we compare what was requested (cpus, memory, time) against what
    was actually used (%cpu, peak_rss, realtime).

    The aggregate efficiencies are ratios of summed totals:
      - CPU efficiency is runtime-weighted in core-hours
      - Memory efficiency is runtime-weighted in GB-hours
      - Time efficiency is weighted by requested time

    Args:
        work: Prepared task-level DataFrame with derived resource and efficiency columns

    Tasks with zero memory or zero realtime are excluded because they would
    produce division-by-zero errors or meaningless efficiency values.

    Returns a DataFrame with one row per process.
    """
    if work.empty:
        return pd.DataFrame()

    if verbose:
        _report_efficiency_exceedances(work)

    metrics = (
        work.groupby("process")
        .agg(
            used_runtime_h=("runtime_hours", "sum"),
            cpu_h=("cpu_h", "sum"),
            memory_h=("memory_h", "sum"),
            cpu_requested_h=("cpu_requested_h", "sum"),
            memory_requested_h=("memory_requested_h", "sum"),
            time_requested_h=("time_requested_hours", "sum"),
        )
        .reset_index()
    )

    metrics["cpu_efficiency"] = (
        metrics["cpu_h"] / metrics["cpu_requested_h"]
    ) * 100.0
    metrics["mem_efficiency"] = (
        metrics["memory_h"] / metrics["memory_requested_h"]
    ) * 100.0
    metrics["time_efficiency"] = (
        metrics["used_runtime_h"] / metrics["time_requested_h"]
    ) * 100.0
    metrics["cpu_efficiency"] = metrics["cpu_efficiency"].replace([np.inf, -np.inf], np.nan)
    metrics["mem_efficiency"] = metrics["mem_efficiency"].replace([np.inf, -np.inf], np.nan)
    metrics["time_efficiency"] = metrics["time_efficiency"].replace([np.inf, -np.inf], np.nan)

    return (
        metrics.drop(columns=["used_runtime_h", "cpu_h", "memory_h"])
        .sort_values("process")
        .reset_index(drop=True)
    )


def compute_per_process_variability_metrics(work: pd.DataFrame) -> pd.DataFrame:
    """Compute per-process variability and task-level spread metrics."""
    if work.empty:
        return pd.DataFrame(columns=["process"])

    metrics = (
        work.groupby("process")
        .agg(
            avg_cpus_used=("cpu_used_cores", "mean"),
            std_cpus_used=("cpu_used_cores", "std"),
            avg_mem_used_gb=("mem_used_gb", "mean"),
            std_mem_used_gb=("mem_used_gb", "std"),
            avg_runtime_hours=("runtime_hours", "mean"),
            std_runtime_hours=("runtime_hours", "std"),
        )
        .reset_index()
    )

    fill_zero_cols = ["std_cpus_used", "std_mem_used_gb", "std_runtime_hours"]
    for col in fill_zero_cols:
        metrics[col] = metrics[col].fillna(0)

    return metrics


def _normalize_series(series: pd.Series) -> pd.Series:
    """Normalize a series to the range [0, 1]."""
    series = pd.Series(series).fillna(0)
    max_val = series.max()
    if max_val <= 0 or pd.isna(max_val):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return series / max_val


def _safe_cv(mean: pd.Series, std: pd.Series) -> pd.Series:
    """Return coefficient of variation while handling zero means safely."""
    mean = pd.Series(mean).replace(0, np.nan)
    std = pd.Series(std).fillna(0)
    return std / mean


def _normalize_weight_group(weights: Dict[str, float]) -> Dict[str, float]:
    """Normalize a weight dictionary so the values sum to 1."""
    total = sum(weights.values())
    if total <= 0:
        n = len(weights)
        return {k: 1 / n for k in weights}
    return {k: v / total for k, v in weights.items()}


def add_optimization_priority_columns(
    metrics: pd.DataFrame,
    impact_weights: Dict[str, float] | None = None,
    inefficiency_weights: Dict[str, float] | None = None,
    variability_weights: Dict[str, float] | None = None,
) -> pd.DataFrame:
    """Add optimization-priority components to a per-process summary table."""
    metrics = metrics.copy()

    if impact_weights is None:
        impact_weights = {
            "cpu_cost": 0.4,
            "mem_cost": 0.3,
            "time_cost": 0.3,
        }

    if inefficiency_weights is None:
        inefficiency_weights = {
            "cpu_ineff": 0.4,
            "mem_ineff": 0.3,
            "time_ineff": 0.3,
        }

    if variability_weights is None:
        variability_weights = {
            "runtime_cv": 0.6,
            "cpu_cv": 0.2,
            "mem_cv": 0.2,
        }

    impact_weights = _normalize_weight_group(impact_weights)
    inefficiency_weights = _normalize_weight_group(inefficiency_weights)
    variability_weights = _normalize_weight_group(variability_weights)

    metrics["_cpu_cost"] = metrics["cpu_h"]
    metrics["_mem_cost"] = metrics["memory_h"]
    metrics["_time_cost"] = metrics["total_runtime_hours"]

    metrics["_cpu_cost_norm"] = _normalize_series(metrics["_cpu_cost"])
    metrics["_mem_cost_norm"] = _normalize_series(metrics["_mem_cost"])
    metrics["_time_cost_norm"] = _normalize_series(metrics["_time_cost"])

    metrics["_impact"] = (
        impact_weights["cpu_cost"] * metrics["_cpu_cost_norm"] +
        impact_weights["mem_cost"] * metrics["_mem_cost_norm"] +
        impact_weights["time_cost"] * metrics["_time_cost_norm"]
    )

    metrics["_cpu_ineff"] = 1 - metrics["cpu_efficiency"] / 100
    metrics["_mem_ineff"] = 1 - metrics["mem_efficiency"] / 100
    metrics["_time_ineff"] = 1 - metrics["time_efficiency"] / 100

    metrics["_inefficiency"] = (
        inefficiency_weights["cpu_ineff"] * metrics["_cpu_ineff"] +
        inefficiency_weights["mem_ineff"] * metrics["_mem_ineff"] +
        inefficiency_weights["time_ineff"] * metrics["_time_ineff"]
    )

    metrics["_runtime_cv"] = _safe_cv(metrics["avg_runtime_hours"], metrics["std_runtime_hours"])
    metrics["_cpu_cv"] = _safe_cv(metrics["avg_cpus_used"], metrics["std_cpus_used"])
    metrics["_mem_cv"] = _safe_cv(metrics["avg_mem_used_gb"], metrics["std_mem_used_gb"])

    metrics["_runtime_cv_norm"] = _normalize_series(metrics["_runtime_cv"].fillna(0))
    metrics["_cpu_cv_norm"] = _normalize_series(metrics["_cpu_cv"].fillna(0))
    metrics["_mem_cv_norm"] = _normalize_series(metrics["_mem_cv"].fillna(0))

    metrics["_variability"] = (
        variability_weights["runtime_cv"] * metrics["_runtime_cv_norm"] +
        variability_weights["cpu_cv"] * metrics["_cpu_cv_norm"] +
        variability_weights["mem_cv"] * metrics["_mem_cv_norm"]
    )

    metrics["priority"] = metrics["_impact"] * (
        metrics["_inefficiency"] + metrics["_variability"]
    )

    return metrics


def build_per_process_summary(
    df: pd.DataFrame, verbose: bool = True
) -> pd.DataFrame:
    """Build the complete per-process summary table."""
    general = compute_per_process_general_summary(df)
    work = add_efficiency_columns(df)
    valid_work = work[(work["memory"] > 0) & (work["realtime"] > 0)].copy()
    usage = compute_per_process_usage_metrics(work)
    efficiency = compute_per_process_efficiency_metrics(valid_work, verbose=verbose)
    variability = compute_per_process_variability_metrics(valid_work)
    summary = (
        general.merge(usage, on="process", how="left")
        .merge(efficiency, on="process", how="left")
        .merge(variability, on="process", how="left", suffixes=("", "_dup"))
    )
    dup_cols = [col for col in summary.columns if col.endswith("_dup")]
    if dup_cols:
        summary = summary.drop(columns=dup_cols)
    return summary

# ══════════════════════════════════════════════════════════════════════════════
# PROCESS CONSISTENCY
# ══════════════════════════════════════════════════════════════════════════════
 
def identify_consistent_processes(process_df, run_files, run_labels, verbose=True):
    """Identify processes present in all runs for cross-run comparison.
    
    Compares processes across multiple pipeline runs and identifies which ones appear
    in every run (consistent) vs. only some runs (inconsistent).
    
    Args:
        process_df: DataFrame with per-process metrics, must have 'process' and 'filename' columns
        run_files: List of filename stems (without path/extension) for each run
        run_labels: List of human-readable labels for each run (for reporting)
        verbose: If True, prints a summary of excluded processes
        
    Returns:
        List of process names that appear in all runs (consistent processes)
    """
    n_runs = len(run_files)
    
    # Count how many runs each process appears in
    process_counts = process_df.groupby("process")["filename"].nunique()
    consistent_processes = process_counts[process_counts == n_runs].index
    missing_processes = process_counts[process_counts < n_runs].index
    
    if verbose:
        if len(missing_processes) > 0:
            print(f"{'='*70}")
            print(f"  EXCLUDED — processes not present in all {n_runs} run(s):")
            print(f"{'─'*70}")
            for proc in sorted(missing_processes):
                runs_with = process_df[process_df["process"] == proc]["filename"].tolist()
                run_labels_with = [run_labels[run_files.index(f)] for f in runs_with]
                print(f"  • {proc}")
                print(f"    Found in: {', '.join(run_labels_with)}")
            print(f"{'─'*70}")
            print(
                f"  Excluded {len(missing_processes)} process(es) — "
                f"keeping {len(consistent_processes)} present in all runs."
            )
            print(f"{'='*70}")
        else:
            print(f"✓ All processes are present in all {n_runs} run(s) — no exclusions.")
        
        print(f"\n✓ {len(consistent_processes)} processes retained for cross-run analysis.")
    
    return consistent_processes.tolist()


def annotate_consistent_processes(
    process_df: pd.DataFrame, verbose: bool = True
) -> pd.DataFrame:
    """Add an ``is_consistent`` flag showing whether each process is present in all runs."""
    process_df = process_df.copy()

    runs = process_df[["filename", "label"]].drop_duplicates()
    filenames = runs["filename"].tolist()
    labels = runs["label"].tolist()

    if len(filenames) == 1:
        consistent_procs = sorted(process_df["process"].unique())
    else:
        consistent_procs = identify_consistent_processes(
            process_df, filenames, labels, verbose=False
        )
    process_df["is_consistent"] = process_df["process"].isin(consistent_procs)

    if verbose:
        _print_consistency_summary(process_df, filenames, labels, consistent_procs)

    return process_df

# ══════════════════════════════════════════════════════════════════════════════
# PLOTTING FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def create_process_composition_plot(pp, run_label, sort_by):
    """Plot per-process burdens as ranked horizontal small multiples."""
    plot_cols = {
        "cpu_h": "CPU burden (core-h)",
        "memory_h": "Memory burden proxy (GB-h)",
        "total_runtime_hours": "Runtime (h)",
        "io_gb": "I/O volume (GB)",
    }
    if sort_by not in plot_cols:
        raise ValueError(
            f"sort_by must be one of {list(plot_cols)}, got {sort_by!r}"
        )

    pp_sorted = pp.sort_values(sort_by, ascending=False).head(12).copy()
    if "process_display" not in pp_sorted.columns:
        pp_sorted["process_display"] = pp_sorted["process"]
    if "process_name" not in pp_sorted.columns:
        pp_sorted["process_name"] = pp_sorted["process"]

    pp_sorted = pp_sorted.iloc[::-1].reset_index(drop=True)

    subplot_specs = [
        ("cpu_h", "CPU burden (core-h)", "#E63946", "core-h"),
        ("memory_h", "Memory burden proxy (GB-h)", "#457B9D", "GB-h"),
        ("total_runtime_hours", "Runtime (h)", "#222222", "h"),
        ("io_gb", "I/O volume (GB)", "#2A9D8F", "GB"),
    ]

    fig = make_subplots(
        rows=1,
        cols=4,
        shared_yaxes=True,
        horizontal_spacing=0.06,
        subplot_titles=[title for _, title, _, _ in subplot_specs],
    )

    customdata = np.stack(
        [
            pp_sorted["cpu_h"],
            pp_sorted["memory_h"],
            pp_sorted["total_runtime_hours"],
            pp_sorted["io_gb"],
            pp_sorted["n_tasks"],
        ],
        axis=-1,
    )

    hover_template = (
        "<b>%{hovertext}</b><br>"
        "Panel value: %{x:.2f} %{meta}<br>"
        "CPU burden: %{customdata[0]:.2f} core-h<br>"
        "Memory burden proxy: %{customdata[1]:.2f} GB-h<br>"
        "Runtime: %{customdata[2]:.2f} h<br>"
        "I/O volume: %{customdata[3]:.2f} GB<br>"
        "Tasks: %{customdata[4]:.0f}<extra></extra>"
    )

    for col_idx, (metric_col, title, color, unit) in enumerate(subplot_specs, start=1):
        fig.add_trace(
            go.Bar(
                x=pp_sorted[metric_col],
                y=pp_sorted["process_display"],
                orientation="h",
                marker=dict(color=color),
                hovertext=pp_sorted["process_name"],
                customdata=customdata,
                meta=unit,
                hovertemplate=hover_template,
                showlegend=False,
            ),
            row=1,
            col=col_idx,
        )
        fig.update_xaxes(title_text=unit, row=1, col=col_idx)

    fig.update_layout(
        title=f"Per-Process Resource Burdens — {run_label}<br><sup>Top 12 processes sorted by {plot_cols[sort_by]}</sup>",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=500,
        bargap=0.2,
    )

    fig.update_yaxes(title_text="Process", automargin=True, row=1, col=1)

    return fig



def _build_process_color_map(processes: List[str]) -> Dict[str, str]:
    """Assign stable colors to processes across interactive plots."""
    palette = [
        "#264653",
        "#2A9D8F",
        "#457B9D",
        "#6D597A",
        "#E9C46A",
        "#F4A261",
        "#E76F51",
        "#8AB17D",
        "#5E548E",
        "#BC4749",
        "#669BBC",
        "#A3B18A",
        "#B56576",
        "#3D5A80",
        "#7F5539",
        "#588157",
    ]
    return {proc: palette[i % len(palette)] for i, proc in enumerate(processes)}


def create_process_scaling_explorer(
    process_df: pd.DataFrame,
    run_summary_df: pd.DataFrame,
    top_n: int = 15,
) -> go.Figure | None:
    """Create an interactive scaling explorer for consistent processes."""
    long_df = process_df[process_df["is_consistent"]].copy()

    if long_df.empty:
        return None

    run_labels = run_summary_df["label"].tolist()
    long_df["run_label"] = long_df["label"]
    run_order = {label: idx for idx, label in enumerate(run_labels)}
    long_df["run_idx"] = long_df["run_label"].map(run_order)

    resource_config = [
        ("CPU", "cpu_h", "CPU burden (core-h)", "core-h", "#E63946"),
        (
            "Memory",
            "memory_h",
            "Memory burden proxy (GB-h)",
            "GB-h",
            "#457B9D",
        ),
        ("Runtime", "total_runtime_hours", "Runtime (h)", "h", "#1D3557"),
        ("I/O", "io_gb", "I/O volume (GB)", "GB", "#2A9D8F"),
    ]

    top_processes_by_resource = {}
    ordered_processes: List[str] = []
    for resource_name, metric_col, _, _, _ in resource_config:
        top_processes = (
            long_df.groupby("process")[metric_col]
            .sum()
            .nlargest(top_n)
            .index.tolist()
        )
        top_processes_by_resource[resource_name] = top_processes
        ordered_processes.extend(top_processes)

    all_processes = list(dict.fromkeys(ordered_processes))
    process_colors = _build_process_color_map(all_processes)
    process_display = (
        long_df.sort_values("run_idx")
        .drop_duplicates("process")
        .set_index("process")["process_display"]
        .to_dict()
    )

    fig = go.Figure()
    trace_ranges: Dict[str, Tuple[int, int]] = {}

    for resource_name, metric_col, y_title, unit, accent_color in resource_config:
        start = len(fig.data)
        for proc in top_processes_by_resource[resource_name]:
            subset = long_df[long_df["process"] == proc].sort_values("run_idx")
            if subset.empty:
                continue

            display_name = process_display.get(proc, proc)
            customdata = np.stack(
                [
                    subset["cpu_h"],
                    subset["memory_h"],
                    subset["total_runtime_hours"],
                    subset["io_gb"],
                    subset["run_label"],
                    subset["n_tasks"],
                ],
                axis=-1,
            )

            fig.add_trace(
                go.Scatter(
                    x=subset["run_label"],
                    y=subset[metric_col],
                    mode="lines+markers",
                    name=display_name,
                    legendgroup=proc,
                    showlegend=True,
                    customdata=customdata,
                    hovertemplate=(
                        f"<b>{display_name}</b><br>"
                        "<i>%{customdata[4]}</i><br><br>"
                        f"<span style='color:{accent_color}'><b>{resource_name}</b></span>: "
                        f"%{{y:.3f}} {unit}<br>"
                        "CPU burden: %{customdata[0]:.3f} core-h<br>"
                        "Memory burden proxy: %{customdata[1]:.3f} GB-h<br>"
                        "Runtime: %{customdata[2]:.3f} h<br>"
                        "I/O volume: %{customdata[3]:.3f} GB<br>"
                        "Tasks: %{customdata[5]:.0f}<extra></extra>"
                    ),
                    marker=dict(
                        size=8,
                        color=process_colors.get(proc, "#6C757D"),
                        line=dict(width=1.5, color="white"),
                    ),
                    line=dict(
                        width=2.5,
                        color=process_colors.get(proc, "#6C757D"),
                        shape="spline",
                        smoothing=0.35,
                    ),
                    visible=resource_name == "CPU",
                )
            )

        trace_ranges[resource_name] = (start, len(fig.data))

    total_traces = len(fig.data)

    def _visible_traces(active_resource: str) -> List[bool]:
        visibility = [False] * total_traces
        start, end = trace_ranges[active_resource]
        for idx in range(start, end):
            visibility[idx] = True
        return visibility

    buttons = [
        dict(
            label=resource_name,
            method="update",
            args=[
                {"visible": _visible_traces(resource_name)},
                {
                    "yaxis.title.text": y_title,
                    "title.text": (
                        "Process Scaling Explorer"
                        f"<br><sup>Top {top_n} consistent processes ranked by {resource_name}</sup>"
                    ),
                },
            ],
        )
        for resource_name, _, y_title, _, _ in resource_config
    ]

    fig.update_layout(
        title=dict(
            text=(
                "Process Scaling Explorer"
                f"<br><sup>Top {top_n} consistent processes ranked by CPU</sup>"
            ),
            x=0.01,
            xanchor="left",
            y=0.95,
            yanchor="top",
        ),
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                x=0.01,
                xanchor="left",
                y=1.10,
                yanchor="bottom",
                pad=dict(r=8, t=6, b=0),
                buttons=buttons,
                showactive=True,
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#D9E2EC",
                borderwidth=1,
                font=dict(size=12, color="#243B53"),
                active=0,
            )
        ],
        xaxis=dict(
            title="Pipeline Run",
            tickfont=dict(size=11, color="#334E68"),
            showgrid=False,
            linecolor="#BCCCDC",
            linewidth=1,
            showline=True,
            ticks="outside",
            ticklen=5,
        ),
        yaxis=dict(
            title="CPU burden (core-h)",
            tickfont=dict(size=10, color="#334E68"),
            gridcolor="#E6EEF5",
            gridwidth=1,
            zeroline=False,
            linecolor="#BCCCDC",
            linewidth=1,
            showline=True,
        ),
        hovermode="closest",
        hoverlabel=dict(
            bgcolor="rgba(255,255,255,0.98)",
            bordercolor="#D9E2EC",
            font=dict(size=11, color="#102A43"),
            namelength=0,
        ),
        height=620,
        width=1180,
        plot_bgcolor="#F8FBFD",
        paper_bgcolor="white",
        legend=dict(
            title="Process",
            font=dict(size=10, color="#243B53"),
            bgcolor="rgba(255,255,255,0.96)",
            bordercolor="#D9E2EC",
            borderwidth=1,
            groupclick="togglegroup",
            itemsizing="constant",
            tracegroupgap=2,
            x=1.02,
            xanchor="left",
            y=1,
            yanchor="top",
        ),
        margin=dict(t=145, r=240, l=80, b=75),
    )

    return fig


def create_optimization_priority_plot(
    metrics,
    impact_weights=None,
    inefficiency_weights=None,
    variability_weights=None,
    top_n=15,
):
    """
    Optimization priority plot based on:
        priority = impact * (inefficiency + variability)

    impact_weights: dict with keys cpu_cost, mem_cost, time_cost
    inefficiency_weights: dict with keys cpu_ineff, mem_ineff, time_ineff
    variability_weights: dict with keys runtime_cv, cpu_cv, mem_cv
    """

    metrics = metrics.copy()

    if "process_display" not in metrics.columns:
        if "process_name" in metrics.columns:
            metrics["process_display"] = metrics["process_name"]
        else:
            metrics["process_display"] = metrics["process"]
    metrics = add_optimization_priority_columns(
        metrics,
        impact_weights=impact_weights,
        inefficiency_weights=inefficiency_weights,
        variability_weights=variability_weights,
    )

    df = metrics.sort_values("priority", ascending=False).head(top_n)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["process"],
            y=df["priority"],
            name="Optimization Priority",
            hovertext=df["process_name"],
            customdata=df[[
                "_impact", "_inefficiency", "_variability",
                "_cpu_cost", "_mem_cost", "_time_cost",
                "_cpu_cost_norm", "_mem_cost_norm", "_time_cost_norm",
                "_cpu_ineff", "_mem_ineff", "_time_ineff",
                "_runtime_cv", "_cpu_cv", "_mem_cv",
                "_runtime_cv_norm", "_cpu_cv_norm", "_mem_cv_norm",
                "n_tasks",
            ]].values,
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Priority: %{y:.3f}<br><br>"
                "<b>Main components</b><br>"
                "Impact: %{customdata[0]:.3f}<br>"
                "Inefficiency: %{customdata[1]:.3f}<br>"
                "Variability: %{customdata[2]:.3f}<br><br>"

                "<b>Impact details</b><br>"
                "CPU cost: %{customdata[3]:.2f} core-h (norm: %{customdata[6]:.2f})<br>"
                "MEM cost: %{customdata[4]:.2f} GB-h (norm: %{customdata[7]:.2f})<br>"
                "TIME cost: %{customdata[5]:.2f} h (norm: %{customdata[8]:.2f})<br><br>"

                "<b>Inefficiency details</b><br>"
                "CPU ineff.: %{customdata[9]:.2f}<br>"
                "MEM ineff.: %{customdata[10]:.2f}<br>"
                "TIME ineff.: %{customdata[11]:.2f}<br><br>"

                "<b>Variability details</b><br>"
                "Runtime CV: %{customdata[12]:.2f} (norm: %{customdata[15]:.2f})<br>"
                "CPU CV: %{customdata[13]:.2f} (norm: %{customdata[16]:.2f})<br>"
                "MEM CV: %{customdata[14]:.2f} (norm: %{customdata[17]:.2f})<br><br>"

                "Tasks: %{customdata[18]:.0f}"
                "<extra></extra>"
            ),
            marker=dict(color="#e76f51")
        )
    )

    fig.update_layout(
        title="Optimization Priority (Impact × (Inefficiency + Variability))",
        xaxis_title="Process",
        yaxis_title="Priority Score",
        plot_bgcolor="white",
        paper_bgcolor="white",
        #margin=dict(t=120, b=120, l=80, r=40),
        hovermode="closest",
        #hoverlabel=dict(font_size=10)
    )

    label_map = dict(zip(df["process"], df["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
        automargin=True,
    )

    return fig
