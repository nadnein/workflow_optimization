
"""
Nextflow trace analysis helper functions.
 
This module provides utilities for loading, preprocessing, and analyzing
Nextflow trace files to identify resource optimization opportunities and
compare resource usage across pipeline runs.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
 
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.colors
import plotly.express as px


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

RESOURCE_CONFIG = {
    "CPU": {
        "contrib_col": "cpu_contrib",
        "time_col": "cpu_time",
        "unit": "core-h",
        "color": "#E63946",
        "convert": lambda x: x / (1000 * 3600),  # ms → hours
        "ylabel": "CPU (core-h)",
        "marker": "o",
    },
    "Memory": {
        "contrib_col": "mem_contrib",
        "time_col": "memory_time",
        "unit": "GB-h",
        "color": "#457B9D",
        "convert": lambda x: x / (1024**3 * 1000 * 3600),  # byte-ms → GB-h
        "ylabel": "Memory (GB-h)",
        "marker": "s",
    },
    "I/O": {
        "contrib_col": "io_contrib",
        "time_col": "io_time",
        "unit": "GB-h",
        "color": "#2A9D8F",
        "convert": lambda x: x / (1024**3 * 1000 * 3600),  # byte-ms → GB-h
        "ylabel": "I/O (GB-h)",
        "marker": "^",
    },
}


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

def load_comparison_groups(comparison_groups: Dict) -> Tuple[pd.DataFrame, pd.DataFrame, List[Path], List[str]]:
    """
    Load and process multiple comparison groups of trace files.
    
    Args:
        comparison_groups: Dict mapping group names to trace file configs
            Each group should have:
                - trace_files: List of Path objects to trace files
                - labels: Optional list of labels (uses filenames if not provided)
        
    Returns:
        Tuple of (run_summary_df, process_df, all_trace_files, all_labels)
    """
    print(f"Loading {len(comparison_groups)} comparison group(s):")
    for group_name in comparison_groups:
        group = comparison_groups[group_name]
        print(f"  • {group_name}: {len(group['trace_files'])} trace file(s)")
    print()
 
    run_results = []
    process_records = []
    all_trace_files = []
    all_labels = []
 
    for group_name, group_config in comparison_groups.items():
        trace_files_group = group_config["trace_files"]
        labels_group = group_config.get("labels")
 
        # Use filenames if no labels provided
        if not labels_group or len(labels_group) != len(trace_files_group):
            labels_group = [f.stem for f in trace_files_group]
 
        print(f"Loading {group_name}:")
        for trace_file, label in zip(trace_files_group, labels_group):
            print(f"  • {trace_file.name}")
            df_trace = load_and_preprocess_trace(trace_file, verbose=False)
            print(f"    → {len(df_trace)} completed tasks")
 
            # Run-level totals
            run_metrics = compute_resource_contributions(df_trace)
            run_metrics["filename"] = trace_file.stem
            run_metrics["group"] = group_name
            run_metrics["label"] = label
            run_results.append(run_metrics)
 
            # Per-process metrics
            pp_contrib = compute_per_process_contributions(df_trace)
            pp_eff = compute_per_process_efficiency_metrics(df_trace)
            pp = pp_contrib.merge(pp_eff, on="process", how="left")
            pp["filename"] = trace_file.stem
            pp["group"] = group_name
            pp["label"] = label
            pp["process_display"] = make_display_labels(
                pp["process"], max_len=20
            )
            process_records.append(pp)
 
            all_trace_files.append(trace_file)
            all_labels.append(label)
        print()
 
    run_summary_df = pd.DataFrame(run_results)
    process_df = pd.concat(process_records, ignore_index=True)
 
    print(
        f"✓ Ready: {len(run_summary_df)} run(s) across {len(comparison_groups)} group(s)"
    )
 
    return run_summary_df, process_df, all_trace_files, all_labels

# ══════════════════════════════════════════════════════════════════════════════
# RESOURCE CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════════
 
def compute_resource_contributions(df):
    """Compute run-level CPU, Memory, and I/O contribution scores.

    All three metrics represent total resource consumption weighted by time:

      cpu_time    = sum(realtime × cores_used)  — total CPU-core-milliseconds
      memory_time = sum(realtime × bytes_used)  — total byte-milliseconds of memory
      io_time     = sum(realtime × bytes_io)    — total byte-milliseconds of I/O

    Returns a plain dict — one row's worth of data — intended to be collected
    into a run-summary DataFrame alongside other runs.
    """
    # CPU: convert %cpu to number of cores (e.g., 500% → 5.0 cores), then weight by wall-clock time
    cpu_time = (df["realtime"] * (df["%cpu"] / 100.0)).sum()

    # Memory: total byte-milliseconds (memory usage × time)
    memory_time = (df["peak_rss"] * df["realtime"]).sum()

    # I/O: total byte-milliseconds (I/O bytes × time)
    # Prefer read_bytes/write_bytes (actual disk bytes); fall back to
    # rchar/wchar (kernel I/O characters) if byte columns are absent
    if "read_bytes" in df.columns and "write_bytes" in df.columns:
        io_bytes = df["read_bytes"].fillna(0) + df["write_bytes"].fillna(0)
        io_time = (io_bytes * df["realtime"]).sum()
    elif "rchar" in df.columns and "wchar" in df.columns:
        io_bytes = df["rchar"].fillna(0) + df["wchar"].fillna(0)
        io_time = (io_bytes * df["realtime"]).sum()
    else:
        io_time = 0  # trace has no I/O columns at all

    total_runtime = df["realtime"].sum()

    # Compute absolute total I/O bytes for informational purposes
    if "read_bytes" in df.columns:
        total_io_bytes = (df["read_bytes"].fillna(0) + df["write_bytes"].fillna(0)).sum()
    else:
        total_io_bytes = (df["rchar"].fillna(0) + df["wchar"].fillna(0)).sum()

    return {
        "cpu_time": cpu_time,                   # total CPU-core-milliseconds across all tasks
        "memory_time": memory_time,             # total byte-milliseconds of memory usage across all tasks
        "io_time": io_time,                     # total byte-milliseconds of I/O across all tasks   
        "total_runtime": total_runtime,         # sum of all task wall-clock times (ms)
        "total_io_bytes": total_io_bytes,       # raw byte count for reference
        "task_count": len(df),
    }


def compute_per_process_contributions(df):
    """Break down the run-level resource contributions per process.

    Uses the same weighting scheme as compute_resource_contributions —
    but groups the results by process name instead of summing across the
    whole run. This lets you see which processes dominate each resource type.

    Returns a DataFrame with one row per process and columns:
        process, cpu_contrib, mem_contrib, io_contrib
    """
    # Work on a copy so we don't mutate the caller's DataFrame
    work = df.copy()

    # Determine the I/O byte column to use (same fallback logic as run-level)
    if "read_bytes" in work.columns and "write_bytes" in work.columns:
        io_bytes = work["read_bytes"].fillna(0) + work["write_bytes"].fillna(0)
    elif "rchar" in work.columns and "wchar" in work.columns:
        io_bytes = work["rchar"].fillna(0) + work["wchar"].fillna(0)
    else:
        io_bytes = pd.Series(0, index=work.index)  # no I/O data available

    # Compute per-task contributions before grouping
    work["cpu_contrib"] = work["realtime"] * (work["%cpu"] / 100.0)
    work["mem_contrib"] = work["peak_rss"] * work["realtime"]
    work["io_contrib"] = io_bytes * work["realtime"]

    # Sum contributions across all task instances of each process
    return (
        work.groupby("process")
        .agg(
            cpu_contrib=("cpu_contrib", "sum"),
            mem_contrib=("mem_contrib", "sum"),
            io_contrib=("io_contrib", "sum"),
        )
        .reset_index()
    )


def compute_per_process_efficiency_metrics(df, cpu_waste_weight=10.0):
    """Compute how efficiently each process uses its requested CPU, memory, and time.

    For each task we compare what was *requested* (cpus, memory, time) against what
    was *actually used* (%cpu, peak_rss, realtime) and convert the gap into wasted
    resource-hours — a concrete, cost-relevant number:

      cpu_hours_wasted     = (cpus_requested - cpus_used)  × runtime_hours
      mem_gb_hours_wasted  = (mem_requested_gb - mem_used_gb) × runtime_hours
      time_hours_wasted    = time_requested - time_used (in hours)

    These are then aggregated per process and combined into a single
    total_waste score that ranks processes from highest to lowest
    optimisation priority.

    Args:
        df: DataFrame with Nextflow trace data
        cpu_waste_weight: Scaling factor for CPU-hours in total_waste calculation.
                         Adjust based on your infrastructure's relative cost of CPU vs memory.
                         Default 10.0 means 1 CPU-hour costs ~10× a GB-hour of memory.

    Tasks with zero memory or zero realtime are excluded because they would
    produce division-by-zero errors or meaningless efficiency values.

    Returns a DataFrame sorted by total_waste descending (worst first), with
    one row per process.
    """
    work = df.copy()
    # Drop tasks without valid memory or runtime — can't compute efficiency for them
    work = work[(work["memory"] > 0) & (work["realtime"] > 0)].copy()
    if work.empty:
        return pd.DataFrame()

    # ── Per-task efficiency calculations ─────────────────────────────────────
    work["runtime_hours"] = work["realtime"] / 3_600_000.0  # ms → hours

    # Actual CPU usage in cores (e.g. %cpu=500 means 5.0 cores used)
    work["cpu_used_cores"] = work["%cpu"] / 100.0
    # Efficiency as a percentage of what was requested
    work["cpu_efficiency"] = (work["cpu_used_cores"] / work["cpus"]) * 100.0

    work["mem_requested_gb"] = work["memory"] / (1024**3)   # bytes → GB
    work["mem_used_gb"] = work["peak_rss"] / (1024**3)      # bytes → GB
    work["mem_efficiency"] = (work["mem_used_gb"] / work["mem_requested_gb"]) * 100.0

    # Time efficiency: compare actual runtime against requested time
    work["time_requested_hours"] = work["time"] / 3_600_000.0  # ms → hours
    # Efficiency: how much of the requested time was actually used
    work["time_efficiency"] = (work["runtime_hours"] / work["time_requested_hours"]) * 100.0
    # Wasted time: requested time that wasn't used
    work["time_hours_wasted"] = (work["time_requested_hours"] - work["runtime_hours"]).clip(lower=0)

    # ── Check for resource over-allocation ────────────────────────────────────
    # Warn if tasks used more resources than requested (scheduler didn't enforce limits)
    cpu_exceeded = work["cpu_efficiency"] > 105.0  # 5% tolerance for measurement noise
    mem_exceeded = work["mem_efficiency"] > 105.0
    time_exceeded = work["time_efficiency"] > 105.0
    
    if cpu_exceeded.any():
        n_exceed = cpu_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks used more CPU cores than requested!")
        print(f"   Scheduler may not be enforcing CPU limits properly.")
        exceed_details = work[cpu_exceeded][["process", "cpus", "cpu_used_cores", "cpu_efficiency"]].head(5)
        print(f"   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(f"     {row['process']}: requested {row['cpus']:.1f} cores, used {row['cpu_used_cores']:.1f} ({row['cpu_efficiency']:.1f}%)")
        print()
    
    if mem_exceeded.any():
        n_exceed = mem_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks used more memory than requested!")
        print(f"   Memory limits may not be enforced properly.")
        exceed_details = work[mem_exceeded][["process", "mem_requested_gb", "mem_used_gb", "mem_efficiency"]].head(5)
        print(f"   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(f"     {row['process']}: requested {row['mem_requested_gb']:.2f} GB, used {row['mem_used_gb']:.2f} GB ({row['mem_efficiency']:.1f}%)")
        print()
    
    if time_exceeded.any():
        n_exceed = time_exceeded.sum()
        print(f"⚠️  WARNING: {n_exceed} tasks ran longer than requested time!")
        print(f"   Time limits may not be enforced, or limits are set too tight.")
        exceed_details = work[time_exceeded][["process", "time_requested_hours", "runtime_hours", "time_efficiency"]].head(5)
        print(f"   Examples (showing up to 5):")
        for _, row in exceed_details.iterrows():
            print(f"     {row['process']}: requested {row['time_requested_hours']:.2f} h, took {row['runtime_hours']:.2f} h ({row['time_efficiency']:.1f}%)")
        print()

    # Wasted resources = gap between requested and used, floored at zero
    # (clip prevents negative waste when usage briefly exceeds the request)
    work["cpu_hours_wasted"] = (work["cpus"] - work["cpu_used_cores"]).clip(
        lower=0
    ) * work["runtime_hours"]
    work["mem_gb_hours_wasted"] = (work["mem_requested_gb"] - work["mem_used_gb"]).clip(
        lower=0
    ) * work["runtime_hours"]

    # ── Aggregate per process ─────────────────────────────────────────────────
    metrics = (
        work.groupby("process")
        .agg(
            n_tasks=("process", "size"),
            avg_cpus_requested=("cpus", "mean"),
            avg_cpus_used=("cpu_used_cores", "mean"),
            avg_cpu_efficiency=("cpu_efficiency", "mean"),
            avg_mem_requested_gb=("mem_requested_gb", "mean"),
            avg_mem_used_gb=("mem_used_gb", "mean"),
            avg_mem_efficiency=("mem_efficiency", "mean"),
            avg_time_requested_hours=("time_requested_hours", "mean"),
            avg_time_efficiency=("time_efficiency", "mean"),
            avg_runtime_hours=("runtime_hours", "mean"),
            total_runtime_hours=("runtime_hours", "sum"),
            total_cpu_hours_wasted=("cpu_hours_wasted", "sum"),
            total_mem_gb_hours_wasted=("mem_gb_hours_wasted", "sum"),
            total_time_hours_wasted=("time_hours_wasted", "sum"),
        )
        .reset_index()
    )

    # overall_efficiency = simple average of CPU, memory, and time efficiency (%)
    metrics["overall_efficiency"] = (
        metrics["avg_cpu_efficiency"] + metrics["avg_mem_efficiency"] + metrics["avg_time_efficiency"]
    ) / 3.0

    # total_waste = composite optimisation priority score.
    # CPU-hours are scaled by cpu_waste_weight to reflect their relative cost
    # compared to memory and time (adjust based on your infrastructure pricing).
    metrics["total_waste"] = (
        metrics["total_cpu_hours_wasted"] * cpu_waste_weight
        + metrics["total_mem_gb_hours_wasted"]
        + metrics["total_time_hours_wasted"]
    )
    return metrics.sort_values("total_waste", ascending=False).reset_index(drop=True)

def add_resource_hour_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add resource contribution columns in natural units (hours).
    
    Args:
        df: DataFrame with resource contribution columns (cpu_contrib, mem_contrib, io_contrib)
        
    Returns:
        DataFrame with additional *_h columns (cpu_h, memory_h, i/o_h, total_h)
    """
    df = df.copy()
    for resource, cfg in RESOURCE_CONFIG.items():
        col_name = f"{resource.lower()}_h"
        df[col_name] = cfg["convert"](df[cfg["contrib_col"]])
    df["total_h"] = df["cpu_h"] + df["memory_h"] + df["i/o_h"]
    return df

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

# ══════════════════════════════════════════════════════════════════════════════
# PLOTTING FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def create_optimization_bubble_chart(
    metrics, title="Nextflow Process Optimization Opportunities"
):
    if metrics.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No process metrics available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(size=18)
        )
        return fig

    # Bubble sizes: total runtime
    runtime_values = metrics["total_runtime_hours"].to_numpy()
    if np.nanmax(runtime_values) > np.nanmin(runtime_values):
        norm_runtime = (runtime_values - np.nanmin(runtime_values)) / (
            np.nanmax(runtime_values) - np.nanmin(runtime_values)
        )
        bubble_sizes = 10 + 60 * np.sqrt(norm_runtime)
    else:
        bubble_sizes = np.full(len(metrics), 40)

    # Color: time inefficiency
    time_inefficiency = 100 - metrics["avg_time_efficiency"]

    # Custom hover data
    custom_data = np.column_stack([
        metrics["process"],                  # [0] process name
        metrics["n_tasks"],                  # [1] number of tasks
        metrics["avg_runtime_hours"],        # [2] avg runtime used
        metrics["avg_time_requested_hours"], # [3] avg time requested
        metrics["total_runtime_hours"],      # [4] total runtime
        metrics["avg_cpus_requested"],       # [5] avg CPU requested
        metrics["avg_cpus_used"],            # [6] avg CPU used
        metrics["avg_cpu_efficiency"],       # [7] CPU efficiency %
        metrics["total_cpu_hours_wasted"],   # [8] CPU-hours wasted
        metrics["avg_mem_requested_gb"],     # [9] avg mem requested
        metrics["avg_mem_used_gb"],          # [10] avg mem used
        metrics["avg_mem_efficiency"],       # [11] mem efficiency %
        metrics["total_mem_gb_hours_wasted"],# [12] mem GB-hours wasted
        metrics["avg_time_efficiency"],      # [13] time efficiency %
        metrics["total_time_hours_wasted"],   # [14] time hours wasted
    ])

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=metrics["total_cpu_hours_wasted"],
            y=metrics["total_mem_gb_hours_wasted"],
            mode="markers",
            marker=dict(
                size=bubble_sizes,
                sizemode="diameter",
                color=time_inefficiency,
                colorscale="RdYlGn_r",
                cmin=0,
                cmax=100,
                opacity=0.85,
                line=dict(width=1.5, color="white"),
                colorbar=dict(title="Time inefficiency (%)", thickness=16, len=0.7, x=1.02)
            ),
            hovertext=metrics["process_display"],
            customdata=custom_data,
            hovertemplate=(
                "<b>%{hovertext}</b><br><br>"
                "Tasks: %{customdata[1]}<br>"
                "Total runtime (sum): %{customdata[4]:.2f} h<br><br>"
                "<b>CPU (avg per task):</b> %{customdata[6]:.2f} used / %{customdata[5]:.2f} requested | Efficiency: %{customdata[7]:.1f}%<br>"
                "<b>Memory (avg per task):</b> %{customdata[10]:.2f} used / %{customdata[9]:.2f} requested | Efficiency: %{customdata[11]:.1f}%<br>"
                "<b>Time (avg per task):</b> %{customdata[2]:.2f} used / %{customdata[3]:.2f} requested | Efficiency: %{customdata[13]:.1f}%<br><br>"
                "<b>CPU wasted (total):</b> %{x:.2f} core-h<br>"
                "<b>Memory wasted (total):</b> %{y:.2f} GB·h<br>"
                "<b>Time wasted (total):</b> %{customdata[14]:.2f} h"
                "<extra></extra>"
            ),
            name="Processes"
        )
    )

    fig.update_layout(
        title=dict(
            text=f"{title}<br><sub>Bubble size = Total runtime | Color = Time inefficiency</sub>",
            x=0.5, xanchor="center"
        ),
        xaxis=dict(title="Total CPU-hours wasted", showgrid=True, gridcolor="lightgray"),
        yaxis=dict(title="Total Memory GB-hours wasted", showgrid=True, gridcolor="lightgray"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="closest",
        width=1000,
        height=500,
        showlegend=False,
    )

    fig.add_annotation(
        text="Top-right, large, red = highest-priority for optimisation",
        xref="paper", yref="paper",
        x=0.99, y=0.98, showarrow=False,
        xanchor="right", yanchor="top",
        font=dict(size=11, color="darkred"),
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor="darkred",
        borderwidth=1
    )

    return fig

# =========================================================
# 🔹 1. Efficiency overview (bar chart)
# =========================================================

import ipywidgets as widgets
from ipywidgets import interact, Dropdown
import plotly.graph_objects as go
import pandas as pd

# -------------------------------
# 1️⃣ Function to create bar chart
# -------------------------------
def create_efficiency_bar(metrics, sort_by="overall_efficiency"):
    """
    Bar chart of efficiencies with runtime line.
    
    sort_by options:
      - 'avg_cpu_efficiency'
      - 'avg_mem_efficiency'
      - 'avg_time_efficiency'
      - 'efficiency_sum'  → sum of the three
      - 'total_runtime_hours'
    """


    # Compute sum if needed
    if sort_by == "efficiency_sum":
        metrics = metrics.copy()
        metrics["efficiency_sum"] = (
            metrics["avg_cpu_efficiency"]
            + metrics["avg_mem_efficiency"]
            + metrics["avg_time_efficiency"]
        )
        sort_by_col = "efficiency_sum"
    else:
        sort_by_col = sort_by

    # Sort ascending (worst at top)
    metrics_sorted = metrics.sort_values(sort_by_col, ascending=True)

    # Only show top 15
    metrics_sorted = metrics_sorted.head(15)

    # Melt for bar plotting
    df_melt = metrics_sorted.melt(
        id_vars=["process", "process_display", "total_runtime_hours"],
        value_vars=["avg_cpu_efficiency", "avg_mem_efficiency", "avg_time_efficiency"],
        var_name="metric",
        value_name="efficiency",
    )

    # --- Create figure ---
    fig = go.Figure()

    # Add efficiency bars
    for metric_name, df_metric in df_melt.groupby("metric"):
        fig.add_trace(
            go.Bar(
                x=df_metric["process"],
                y=df_metric["efficiency"],
                name=metric_name.replace("_", " ").title(),
                hovertext=df_metric["process_display"],
                hovertemplate="<b>%{hovertext}</b><br>Efficiency: %{y:.1f}%<extra></extra>",
            )
        )

    # Add runtime line (secondary y-axis)
    fig.add_trace(
        go.Scatter(
            x=metrics_sorted["process"],
            y=metrics_sorted["total_runtime_hours"],
            name="Total Runtime (h)",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="black", width=2, dash="dot"),
            marker=dict(size=6),
            hovertext=metrics_sorted["process_display"],
            hovertemplate="<b>%{hovertext}</b><br>Total Runtime: %{y:.1f} h<extra></extra>",
        )
    )

    # Layout: fill available width, reduce margins, move legend above plot
    fig.update_layout(
        title=f"Process Efficiency & Runtime Overview (sorted by {sort_by})",
        xaxis_title="Process",
        yaxis=dict(title="Efficiency (%)"),
        yaxis2=dict(
            title="Total Runtime (h)",
            overlaying="y",
            side="right",
            #showgrid=False,
        ),
        barmode="group",
        plot_bgcolor="white",
        paper_bgcolor="white",
        autosize=True,
        width=1000,
        height=500,
        #margin=dict(l=40, r=180, t=60, b=40),  # more right margin for legend
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.9,
            xanchor="left",
            x=1.1,
            font=dict(size=12),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#ccc",
            borderwidth=1,
        ),
        #transition=dict(duration=0),  # disables animation for initial render
    )

    # Use display labels on x-axis
    label_map = dict(zip(metrics_sorted["process"], metrics_sorted["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
        automargin=True,
    )

    return fig



# =========================================================
# 🔹 2. Optimization priority (your improved bubble plot)
# =========================================================
def create_bubble(metrics):
    sizes = np.sqrt(metrics["computational_burden"])
    sizes = 10 + 60 * (sizes / sizes.max())

    fig = px.scatter(
        metrics,
        x="total_cpu_hours_wasted",
        y="total_mem_gb_hours_wasted",
        size=sizes,
        color="overall_efficiency",
        hover_name="process_display",
        color_continuous_scale="RdYlGn",
    )

    fig.update_layout(
        title="Optimization Priority Map",
        xaxis_title="CPU-hours wasted",
        yaxis_title="Memory GB-hours wasted",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=500,
        coloraxis_colorbar=dict(title="Efficiency"),
    )

    fig.add_annotation(
        text="Top-right = highest optimization priority",
        xref="paper",
        yref="paper",
        x=0.99,
        y=0.98,
        showarrow=False,
    )

    return fig


# =========================================================
# 🔹 3. Efficiency score vs impact
# =========================================================
def create_score_plot(metrics):
    fig = px.scatter(
        metrics,
        x="overall_efficiency",
        y="computational_burden",
        size="n_tasks",
        hover_name="process_display",
    )

    fig.update_layout(
        title="Efficiency vs Computational Impact",
        xaxis_title="Efficiency Score",
        yaxis_title="Computational Burden",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=400,
    )

    return fig


