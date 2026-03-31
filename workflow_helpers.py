
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
from plotly.subplots import make_subplots


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

RESOURCE_CONFIG = {
    "CPU": {
        "contrib_col": "cpu_contrib",
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
    "IO": {
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
            pp["process_name"] = make_unique_labels(pp["process"])
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
    """Compute per-process resource contributions in *hours* + variability.

    Returns a DataFrame with:
        process,
        cpu_h, memory_h, io_h              → total contribution (sum)
        cpu_h_std, memory_h_std, io_h_std  → variation across tasks
        cpu_h_mean, memory_h_mean, io_h_mean → per-task averages
    """

    work = df.copy()

    # ── Time in hours ─────────────────────────────────────────────────────────
    work["runtime_hours"] = work["realtime"] / 3_600_000.0  # ms → hours

    # ── CPU (core-hours) ──────────────────────────────────────────────────────
    work["cpu_used_cores"] = work["%cpu"] / 100.0
    work["cpu_h"] = work["cpu_used_cores"] * work["runtime_hours"]

    # ── Memory (GB-hours) ─────────────────────────────────────────────────────
    work["mem_used_gb"] = work["peak_rss"] / (1024**3)
    work["memory_h"] = work["mem_used_gb"] * work["runtime_hours"]

    # ── I/O (GB-hours, best-effort) ───────────────────────────────────────────
    if "read_bytes" in work.columns and "write_bytes" in work.columns:
        io_bytes = work["read_bytes"].fillna(0) + work["write_bytes"].fillna(0)
    elif "rchar" in work.columns and "wchar" in work.columns:
        io_bytes = work["rchar"].fillna(0) + work["wchar"].fillna(0)
    else:
        io_bytes = pd.Series(0, index=work.index)

    work["io_gb"] = io_bytes / (1024**3)
    work["io_h"] = work["io_gb"] * work["runtime_hours"]

    # ── Aggregate per process ─────────────────────────────────────────────────
    metrics = (
        work.groupby("process")
        .agg(
            # totals
            cpu_h=("cpu_h", "sum"),
            memory_h=("memory_h", "sum"),
            io_h=("io_h", "sum"),
            
            # means 
            cpu_h_mean=("cpu_h", "mean"),
            memory_h_mean=("memory_h", "mean"),
            io_h_mean=("io_h", "mean"),

            # variability (for error bars)
            cpu_h_std=("cpu_h", "std"),
            memory_h_std=("memory_h", "std"),
            io_h_std=("io_h", "std"),
        )
        .reset_index()
    )

    # ── Handle NaNs (single-task processes) ───────────────────────────────────
    for col in ["cpu_h_std", "memory_h_std", "io_h_std"]:
        metrics[col] = metrics[col].fillna(0)

    return metrics



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
    cpu_exceeded = work["cpu_efficiency"] > 100.0  
    mem_exceeded = work["mem_efficiency"] > 100.0
    time_exceeded = work["time_efficiency"] > 100.0
    
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
            std_cpus_requested=("cpus", "std"),
            avg_cpus_used=("cpu_used_cores", "mean"),
            std_cpus_used=("cpu_used_cores", "std"),
            avg_cpu_efficiency=("cpu_efficiency", "mean"),
            std_cpu_efficiency=("cpu_efficiency", "std"),
            avg_mem_requested_gb=("mem_requested_gb", "mean"),
            std_mem_requested_gb=("mem_requested_gb", "std"),
            avg_mem_used_gb=("mem_used_gb", "mean"),
            std_mem_used_gb=("mem_used_gb", "std"),
            avg_mem_efficiency=("mem_efficiency", "mean"),
            std_mem_efficiency=("mem_efficiency", "std"),
            avg_time_requested_hours=("time_requested_hours", "mean"),
            std_time_requested_hours=("time_requested_hours", "std"),
            avg_time_efficiency=("time_efficiency", "mean"),
            std_time_efficiency=("time_efficiency", "std"),
            avg_runtime_hours=("runtime_hours", "mean"),
            std_runtime_hours=("runtime_hours", "std"),
            total_runtime_hours=("runtime_hours", "sum"),
            std_total_runtime_hours=("runtime_hours", "std"),
            total_cpu_hours_wasted=("cpu_hours_wasted", "sum"),
            total_mem_gb_hours_wasted=("mem_gb_hours_wasted", "sum"),
            total_time_hours_wasted=("time_hours_wasted", "sum"),
        )
        .reset_index()
    )
    # Fill NaN stds (single-task processes) with 0
    std_cols = [
        "std_cpu_efficiency", "std_mem_efficiency", "std_time_efficiency",
        "std_cpus_used", "std_cpus_requested", "std_mem_used_gb", "std_mem_requested_gb",
        "std_time_requested_hours", "std_runtime_hours", "std_total_runtime_hours"
    ]
    for col in std_cols:
        if col in metrics.columns:
            metrics[col] = metrics[col].fillna(0)

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


# -------------------------------
# 1️⃣ Function to create bar chart
# -------------------------------

def calculate_optimization_priority(
    metrics, 
    weight_runtime=0.25,
    weight_inefficiency=0.3,
    weight_cpu_demand=0.15,
    weight_mem_demand=0.15,
    weight_time_demand=0.15
):
    """
    Calculate optimization priority score for each process.
    
    Higher score = better optimization target
    
    Parameters:
    -----------
    metrics : DataFrame
        Process metrics data
    weight_runtime : float (default 0.25)
        Weight for total runtime impact (cumulative time across all jobs)
    weight_inefficiency : float (default 0.3)
        Weight for resource inefficiency (0-1)
    weight_cpu_demand : float (default 0.15)
        Weight for CPU resource demand (0-1)
    weight_mem_demand : float (default 0.15)
        Weight for memory resource demand (0-1)
    weight_time_demand : float (default 0.15)
        Weight for walltime/duration demand per job (0-1)
    
    Note: Weights should sum to 1.0
    
    Returns:
    --------
    DataFrame with added columns:
        - optimization_priority: Score from 0-100
        - priority_rank: Ranking (1 = highest priority)
    """
    # Validate weights
    total_weight = (weight_runtime + weight_inefficiency + weight_cpu_demand + 
                   weight_mem_demand + weight_time_demand)
    if not (0.99 <= total_weight <= 1.01):
        print(f"Warning: Weights sum to {total_weight:.3f}, not 1.0. Normalizing...")
        weight_runtime /= total_weight
        weight_inefficiency /= total_weight
        weight_cpu_demand /= total_weight
        weight_mem_demand /= total_weight
        weight_time_demand /= total_weight
    
    metrics = metrics.copy()
    
    # 1. Normalize total runtime (cumulative hours across all jobs) (0-100 scale)
    if metrics["total_runtime_hours"].max() > 0:
        runtime_norm = (metrics["total_runtime_hours"] / metrics["total_runtime_hours"].max()) * 100
    else:
        runtime_norm = 0
    
    # 2. Calculate average inefficiency (inverse of efficiency)
    avg_efficiency = (
        metrics["avg_cpu_efficiency"] + 
        metrics["avg_mem_efficiency"] + 
        metrics["avg_time_efficiency"]
    ) / 3
    inefficiency_score = 100 - avg_efficiency
    
    # 3. Normalize CPU demand (0-100 scale)
    if metrics["avg_cpus_used"].max() > 0:
        cpu_demand_norm = (metrics["avg_cpus_used"] / metrics["avg_cpus_used"].max()) * 100
    else:
        cpu_demand_norm = 0
    
    # 4. Normalize memory demand (0-100 scale)
    if metrics["avg_mem_used_gb"].max() > 0:
        mem_demand_norm = (metrics["avg_mem_used_gb"] / metrics["avg_mem_used_gb"].max()) * 100
    else:
        mem_demand_norm = 0
    
    # 5. Normalize time/walltime demand (average job duration) (0-100 scale)
    if metrics["avg_runtime_hours"].max() > 0:
        time_demand_norm = (metrics["avg_runtime_hours"] / metrics["avg_runtime_hours"].max()) * 100
    else:
        time_demand_norm = 0
    
    # 6. Calculate weighted priority score
    priority_score = (
        weight_runtime * runtime_norm +
        weight_inefficiency * inefficiency_score +
        weight_cpu_demand * cpu_demand_norm +
        weight_mem_demand * mem_demand_norm +
        weight_time_demand * time_demand_norm
    )
    
    metrics["optimization_priority"] = priority_score
    metrics["priority_rank"] = metrics["optimization_priority"].rank(
        ascending=False, method="dense"
    ).astype(int)
    
    # Store component scores for analysis
    metrics["_runtime_component"] = weight_runtime * runtime_norm
    metrics["_inefficiency_component"] = weight_inefficiency * inefficiency_score
    metrics["_cpu_demand_component"] = weight_cpu_demand * cpu_demand_norm
    metrics["_mem_demand_component"] = weight_mem_demand * mem_demand_norm
    metrics["_time_demand_component"] = weight_time_demand * time_demand_norm
    
    return metrics

def create_optimization_priority_plot(metrics):
    """
    Interactive bar chart showing optimization priority scores with adjustable weight sliders.
    
    Parameters:
    -----------
    metrics : DataFrame
        Process metrics data with efficiency and resource usage columns
    
    Returns:
    --------
    plotly.graph_objects.Figure
        Interactive figure with weight sliders
    """
    
    # Default weights
    DEFAULT_WEIGHTS = {
        'runtime': 0.25,
        'inefficiency': 0.30,
        'cpu_demand': 0.15,
        'mem_demand': 0.15,
        'time_demand': 0.15
    }
    
    def _calculate_priority_scores(metrics, weights):
        """Calculate priority scores with given weights."""
        metrics = metrics.copy()
        
        # 1. Normalize total runtime (0-100)
        runtime_norm = (metrics["total_runtime_hours"] / metrics["total_runtime_hours"].max()) * 100 \
            if metrics["total_runtime_hours"].max() > 0 else 0
        
        # 2. Calculate inefficiency (inverse of avg efficiency)
        avg_efficiency = (
            metrics["avg_cpu_efficiency"] + 
            metrics["avg_mem_efficiency"] + 
            metrics["avg_time_efficiency"]
        ) / 3
        inefficiency_score = 100 - avg_efficiency
        
        # 3. Normalize CPU demand (0-100)
        cpu_demand_norm = (metrics["avg_cpus_used"] / metrics["avg_cpus_used"].max()) * 100 \
            if metrics["avg_cpus_used"].max() > 0 else 0
        
        # 4. Normalize memory demand (0-100)
        mem_demand_norm = (metrics["avg_mem_used_gb"] / metrics["avg_mem_used_gb"].max()) * 100 \
            if metrics["avg_mem_used_gb"].max() > 0 else 0
        
        # 5. Normalize time demand (avg job duration) (0-100)
        time_demand_norm = (metrics["avg_runtime_hours"] / metrics["avg_runtime_hours"].max()) * 100 \
            if metrics["avg_runtime_hours"].max() > 0 else 0
        
        # 6. Calculate weighted priority score
        priority_score = (
            weights['runtime'] * runtime_norm +
            weights['inefficiency'] * inefficiency_score +
            weights['cpu_demand'] * cpu_demand_norm +
            weights['mem_demand'] * mem_demand_norm +
            weights['time_demand'] * time_demand_norm
        )
        
        # Store components for hover info
        metrics["optimization_priority"] = priority_score
        metrics["_runtime_component"] = weights['runtime'] * runtime_norm
        metrics["_inefficiency_component"] = weights['inefficiency'] * inefficiency_score
        metrics["_cpu_demand_component"] = weights['cpu_demand'] * cpu_demand_norm
        metrics["_mem_demand_component"] = weights['mem_demand'] * mem_demand_norm
        metrics["_time_demand_component"] = weights['time_demand'] * time_demand_norm
        
        # Also store normalized values for hover
        metrics["_runtime_norm"] = runtime_norm
        metrics["_inefficiency_score"] = inefficiency_score
        metrics["_cpu_demand_norm"] = cpu_demand_norm
        metrics["_mem_demand_norm"] = mem_demand_norm
        metrics["_time_demand_norm"] = time_demand_norm
        
        return metrics
    
    # Create weight combinations for slider steps
    # We'll create frames for different weight combinations
    weight_steps = []
    
    # Generate reasonable weight variations (0.0 to 0.5 in steps of 0.05)
    weight_values = [i * 0.05 for i in range(11)]  # 0.0, 0.05, ..., 0.50
    
    # For simplicity, create frames for each individual weight slider
    # Each frame will adjust one weight and redistribute others proportionally
    
    # Calculate initial scores
    metrics_default = _calculate_priority_scores(metrics, DEFAULT_WEIGHTS)
    metrics_sorted = metrics_default.sort_values("optimization_priority", ascending=False).head(15)
    
    # Create the base figure
    fig = go.Figure()
    
    # Add main priority bar
    fig.add_trace(
        go.Bar(
            x=metrics_sorted["process"],
            y=metrics_sorted["optimization_priority"],
            name="Optimization Priority",
            marker=dict(color="#1f77b4"),
            hovertext=metrics_sorted["process_display"],
            customdata=metrics_sorted[[
                "_runtime_component", "_runtime_norm",
                "_inefficiency_component", "_inefficiency_score",
                "_cpu_demand_component", "_cpu_demand_norm",
                "_mem_demand_component", "_mem_demand_norm",
                "_time_demand_component", "_time_demand_norm",
                "total_runtime_hours", "avg_cpus_used", "avg_mem_used_gb",
                "avg_runtime_hours", "n_tasks"
            ]].values,
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Priority Score: %{y:.1f}<br><br>"
                "<b>Score Components:</b><br>"
                "Runtime: %{customdata[0]:.1f} (norm: %{customdata[1]:.1f})<br>"
                "Inefficiency: %{customdata[2]:.1f} (score: %{customdata[3]:.1f})<br>"
                "CPU Demand: %{customdata[4]:.1f} (norm: %{customdata[5]:.1f})<br>"
                "Mem Demand: %{customdata[6]:.1f} (norm: %{customdata[7]:.1f})<br>"
                "Time Demand: %{customdata[8]:.1f} (norm: %{customdata[9]:.1f})<br><br>"
                "<b>Actual Values:</b><br>"
                "Total Runtime: %{customdata[10]:.1f} h<br>"
                "Avg CPUs: %{customdata[11]:.2f} cores<br>"
                "Avg Mem: %{customdata[12]:.2f} GB<br>"
                "Avg Job Time: %{customdata[13]:.2f} h<br>"
                "Tasks: %{customdata[14]:.0f}"
                "<extra></extra>"
            ),
        )
    )
    
    # Create frames for weight variations
    frames = []
    slider_steps = []
    
    # We'll create a combined slider that varies all weights together
    # This is a simplified approach - you could create 5 separate sliders if needed
    num_steps = 21  # Number of slider positions
    
    for step_idx in range(num_steps):
        # Interpolate weights based on slider position
        # At step 0: default weights
        # At other steps: emphasize different components
        
        # Simple approach: vary one weight at a time in cycles
        component_idx = (step_idx // 5) % 5
        component_names = ['runtime', 'inefficiency', 'cpu_demand', 'mem_demand', 'time_demand']
        
        # Create weight variation
        if step_idx == 0:
            weights = DEFAULT_WEIGHTS.copy()
            label = "Default (balanced)"
        else:
            weights = DEFAULT_WEIGHTS.copy()
            varied_component = component_names[component_idx]
            
            # Increase the selected component, decrease others
            variation = (step_idx % 5) * 0.05
            weights[varied_component] = min(0.5, DEFAULT_WEIGHTS[varied_component] + variation)
            
            # Redistribute remaining weight
            other_components = [c for c in component_names if c != varied_component]
            remaining_weight = 1.0 - weights[varied_component]
            for comp in other_components:
                weights[comp] = remaining_weight / len(other_components)
            
            label = f"{varied_component.replace('_', ' ').title()} +{variation:.2f}"
        
        # Calculate scores with these weights
        metrics_step = _calculate_priority_scores(metrics, weights)
        metrics_step_sorted = metrics_step.sort_values("optimization_priority", ascending=False).head(15)
        
        # Create frame
        frame_data = go.Bar(
            x=metrics_step_sorted["process"],
            y=metrics_step_sorted["optimization_priority"],
            name="Optimization Priority",
            marker=dict(color="#1f77b4"),
            hovertext=metrics_step_sorted["process_display"],
            customdata=metrics_step_sorted[[
                "_runtime_component", "_runtime_norm",
                "_inefficiency_component", "_inefficiency_score",
                "_cpu_demand_component", "_cpu_demand_norm",
                "_mem_demand_component", "_mem_demand_norm",
                "_time_demand_component", "_time_demand_norm",
                "total_runtime_hours", "avg_cpus_used", "avg_mem_used_gb",
                "avg_runtime_hours", "n_tasks"
            ]].values,
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Priority Score: %{y:.1f}<br><br>"
                "<b>Score Components:</b><br>"
                "Runtime: %{customdata[0]:.1f} (norm: %{customdata[1]:.1f})<br>"
                "Inefficiency: %{customdata[2]:.1f} (score: %{customdata[3]:.1f})<br>"
                "CPU Demand: %{customdata[4]:.1f} (norm: %{customdata[5]:.1f})<br>"
                "Mem Demand: %{customdata[6]:.1f} (norm: %{customdata[7]:.1f})<br>"
                "Time Demand: %{customdata[8]:.1f} (norm: %{customdata[9]:.1f})<br><br>"
                f"<b>Weights:</b> R:{weights['runtime']:.2f} I:{weights['inefficiency']:.2f} "
                f"C:{weights['cpu_demand']:.2f} M:{weights['mem_demand']:.2f} T:{weights['time_demand']:.2f}<br><br>"
                "<b>Actual Values:</b><br>"
                "Total Runtime: %{customdata[10]:.1f} h<br>"
                "Avg CPUs: %{customdata[11]:.2f} cores<br>"
                "Avg Mem: %{customdata[12]:.2f} GB<br>"
                "Avg Job Time: %{customdata[13]:.2f} h<br>"
                "Tasks: %{customdata[14]:.0f}"
                "<extra></extra>"
            ),
        )
        
        frames.append(go.Frame(data=[frame_data], name=str(step_idx)))
        slider_steps.append({
            "args": [[str(step_idx)], {
                "frame": {"duration": 0, "redraw": True},
                "mode": "immediate",
                "transition": {"duration": 0}
            }],
            "label": label,
            "method": "animate"
        })
    
    fig.frames = frames
    
    # Layout
    fig.update_layout(
        title="Optimization Priority Ranking (Interactive Weight Adjustment)",
        xaxis_title="Process",
        yaxis=dict(title="Priority Score (0-100)"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        autosize=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1.0,
            xanchor="left",
            x=1.02,
            font=dict(size=12),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#ccc",
            borderwidth=1,
        ),
        sliders=[{
            "active": 0,
            "yanchor": "top",
            "y": -0.2,
            "xanchor": "left",
            "currentvalue": {
                "prefix": "Weight Profile: ",
                "visible": True,
                "xanchor": "left"
            },
            "pad": {"b": 10, "t": 50},
            "len": 0.9,
            "x": 0.05,
            "steps": slider_steps
        }]
    )
    
    # Set x-axis labels
    label_map = dict(zip(metrics_sorted["process"], metrics_sorted["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
        automargin=True,
    )
    
    return fig

def create_efficiency_dashboard(
    metrics, 
    sort_by="optimization_priority",
    weight_runtime=0.25,
    weight_inefficiency=0.3,
    weight_cpu_demand=0.15,
    weight_mem_demand=0.15,
    weight_time_demand=0.15,
    show_top_n=15
):
    """
    Two-panel dashboard with optimization priority.
    
    sort_by options:
      - 'avg_cpu_efficiency'
      - 'avg_mem_efficiency'
      - 'avg_time_efficiency'
      - 'efficiency_sum'
      - 'total_runtime_hours'
      - 'optimization_priority' (recommended!)
    """
    # Calculate optimization priority
    metrics = calculate_optimization_priority(
        metrics,
        weight_runtime=weight_runtime,
        weight_inefficiency=weight_inefficiency,
        weight_cpu_demand=weight_cpu_demand,
        weight_mem_demand=weight_mem_demand,
        weight_time_demand=weight_time_demand
    )
    
    # Compute efficiency sum if needed
    if sort_by == "efficiency_sum":
        metrics["efficiency_sum"] = (
            metrics["avg_cpu_efficiency"]
            + metrics["avg_mem_efficiency"]
            + metrics["avg_time_efficiency"]
        )
        sort_by_col = "efficiency_sum"
    else:
        sort_by_col = sort_by
    
    # Sort by priority (descending for priority, ascending for efficiency)
    if sort_by == "optimization_priority":
        metrics_sorted = metrics.sort_values(sort_by_col, ascending=False)
    else:
        metrics_sorted = metrics.sort_values(sort_by_col, ascending=True)
    
    metrics_sorted = metrics_sorted.head(show_top_n)
    
    # Melt for bar plotting
    df_melt = metrics_sorted.melt(
        id_vars=["process", "process_display", "total_runtime_hours", "optimization_priority"],
        value_vars=["avg_cpu_efficiency", "avg_mem_efficiency", "avg_time_efficiency"],
        var_name="metric",
        value_name="efficiency",
    )
    
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.6, 0.4],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
        subplot_titles=(
            f"Process Efficiency & Runtime (sorted by {sort_by})",
            "Absolute Resource Usage & Priority Breakdown"
        ),
        vertical_spacing=0.12
    )
    
    # === TOP PANEL: Efficiency bars ===
    for metric_name, df_metric in df_melt.groupby("metric"):
        fig.add_trace(
            go.Bar(
                x=df_metric["process"],
                y=df_metric["efficiency"],
                name=metric_name.replace("avg_", "").replace("_", " ").title(),
                hovertext=df_metric["process_display"],
                hovertemplate="<b>%{hovertext}</b><br>Efficiency: %{y:.1f}%<extra></extra>",
                legendgroup="efficiency",
            ),
            row=1, col=1, secondary_y=False
        )
    
    # Runtime line
    fig.add_trace(
        go.Scatter(
            x=metrics_sorted["process"],
            y=metrics_sorted["total_runtime_hours"],
            name="Total Runtime (h)",
            mode="lines+markers",
            line=dict(color="black", width=2, dash="dot"),
            marker=dict(size=6),
            hovertext=metrics_sorted["process_display"],
            hovertemplate="<b>%{hovertext}</b><br>Runtime: %{y:.1f} h<extra></extra>",
            legendgroup="runtime",
        ),
        row=1, col=1, secondary_y=True
    )
    
    # Optimization priority markers (on primary y-axis as reference)
    fig.add_trace(
        go.Scatter(
            x=metrics_sorted["process"],
            y=metrics_sorted["optimization_priority"],
            name="Optimization Priority",
            mode="markers",
            marker=dict(
                size=12,
                color=metrics_sorted["optimization_priority"],
                colorscale="RdYlGn_r",  # Red = high priority, Green = low
                showscale=True,
                colorbar=dict(
                    title="Priority<br>Score",
                    x=1.15,
                    len=0.3,
                    y=0.8
                ),
                line=dict(width=1, color="black")
            ),
            hovertext=metrics_sorted["process_display"],
            hovertemplate="<b>%{hovertext}</b><br>Priority Score: %{y:.1f}<extra></extra>",
            legendgroup="priority",
            yaxis="y3"
        ),
        row=1, col=1
    )
    
    # === BOTTOM PANEL: Absolute usage for top priority process ===
    top_process = metrics_sorted.iloc[0]
    
    # Resource usage metrics (now including all three demands)
    usage_labels = ["CPU (cores)", "Memory (GB)", "Walltime (h)"]
    usage_values = [
        top_process["avg_cpus_used"],
        top_process["avg_mem_used_gb"],
        top_process["avg_runtime_hours"]
    ]
    
    fig.add_trace(
        go.Bar(
            x=usage_labels,
            y=usage_values,
            name="Avg Resource Usage",
            marker_color=["#1f77b4", "#ff7f0e", "#2ca02c"],
            text=[f"{v:.2f}" for v in usage_values],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>%{y:.2f}<extra></extra>",
            showlegend=False,
        ),
        row=2, col=1
    )
    
    # Add priority score breakdown as annotations
    priority_breakdown = [
        f"Total Runtime: {top_process['_runtime_component']:.1f}",
        f"Inefficiency: {top_process['_inefficiency_component']:.1f}",
        f"CPU Demand: {top_process['_cpu_demand_component']:.1f}",
        f"Mem Demand: {top_process['_mem_demand_component']:.1f}",
        f"Time Demand: {top_process['_time_demand_component']:.1f}",
    ]
    
    # Layout
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        autosize=True,
        width=1200,
        height=900,
        barmode="group",
        hovermode="closest",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=1.02,
            font=dict(size=11),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#ccc",
            borderwidth=1,
        ),
        annotations=[
            dict(
                text=f"<b>Top Priority: {top_process['process_display']}</b><br>" +
                     f"Total Score: {top_process['optimization_priority']:.1f}<br>" +
                     "<br>".join(priority_breakdown),
                xref="paper", yref="paper",
                x=0.02, y=0.35,
                showarrow=False,
                bgcolor="rgba(255,255,200,0.8)",
                bordercolor="black",
                borderwidth=1,
                font=dict(size=10),
                align="left"
            )
        ]
    )
    
    # Add third y-axis for priority scores
    fig.update_layout(
        yaxis3=dict(
            title="Priority Score",
            overlaying="y",
            side="right",
            position=0.95,
            showgrid=False,
            range=[0, 100]
        )
    )
    
    # Update axes
    fig.update_xaxes(title_text="Process", row=1, col=1)
    fig.update_yaxes(title_text="Efficiency (%)", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Runtime (h)", row=1, col=1, secondary_y=True)
    fig.update_xaxes(title_text="Resource Metric", row=2, col=1)
    fig.update_yaxes(title_text="Average Usage per Job", row=2, col=1)
    
    # Use display labels on x-axis
    label_map = dict(zip(metrics_sorted["process"], metrics_sorted["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
        automargin=True,
        row=1, col=1
    )
    
    return fig



def create_process_composition_plot(pp, run_label, sort_by):
    """Plotly version of per-process resource composition."""

    # Sort + select top N
    pp_sorted = pp.sort_values(sort_by, ascending=False).head(12)

    fig = go.Figure()

    resources = [
        ("cpu_h_mean", "cpu_h_std", "CPU", "(core usage x runtime)"),
        ("memory_h_mean", "memory_h_std", "Memory", "(peak RSS x runtime)"),
        ("io_h_mean", "io_h_std", "I/O", "(I/O operations x runtime)"),
    ]
    
    customdata = np.stack(
        [
            pp_sorted["total_runtime_hours"],   
            pp_sorted["n_tasks"],               
        ],
        axis=-1
    )
    
    # --- Bars (CPU / Memory / I/O) ---
    for res_col, std_col, label, calc in resources:
        fig.add_trace(
            go.Bar(
                x=pp_sorted["process"],
                y=pp_sorted[res_col],
                name=label + " " + calc,
                error_y=dict(
                    type="data",
                    array=pp_sorted[std_col],
                    visible=True,
                ),
                customdata=customdata,
                hovertext=pp_sorted["process_name"],
                hovertemplate=(
                    "<b>%{hovertext}</b><br>"
                    f"{label}-hours: %{{y:.2f}} h ± %{{error_y.array:.2f}} h<br>"
                    "Total process runtime: %{customdata[0]:.2f} h<br>"
                    "Tasks: %{customdata[1]}<extra></extra>"
                ),
            )
        )

    # --- Runtime line ---
    if "total_runtime_hours" in pp_sorted.columns:
        fig.add_trace(
            go.Scatter(
                x=pp_sorted["process"],
                y=pp_sorted["total_runtime_hours"],
                name="Total Runtime (h)",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="black", width=2, dash="dot"),
                marker=dict(size=6),
                hovertext=pp_sorted["process_name"],
                hovertemplate="<b>%{hovertext}</b><br>Runtime: %{y:.2f} h<extra></extra>",
            )
        )

    # --- Layout ---
    fig.update_layout(
        title=f"Avg. Per Process Resource Contribution (± std) — {run_label}",
        xaxis_title="Process",
        yaxis=dict(title="Resource (hours)"),
        yaxis2=dict(
            title="Runtime (h)",
            overlaying="y",
            side="right",
        ),
        barmode="group",
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1.0,
            xanchor="left",
            x=1.2,
        ),
    )


    # --- Use display labels ---
    label_map = dict(zip(pp_sorted["process"], pp_sorted["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
        automargin=True,
    )

    return fig



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
    
    # Configuration for metrics
    METRIC_CONFIG = {
        "avg_cpu_efficiency": {
            "used": "avg_cpus_used",
            "requested": "avg_cpus_requested",
            "unit": "cores",
        },
        "avg_mem_efficiency": {
            "used": "avg_mem_used_gb",
            "requested": "avg_mem_requested_gb",
            "unit": "GB",
        },
        "avg_time_efficiency": {
            "used": "avg_runtime_hours",
            "requested": "avg_time_requested_hours",
            "unit": "h",
        },
    }
    
    def _prepare_data(metrics, sort_by):
        """Prepare and sort metrics data."""
        metrics = metrics.copy()
        
        # Compute efficiency sum if needed
        if sort_by == "efficiency_sum":
            metrics["efficiency_sum"] = (
                metrics["avg_cpu_efficiency"]
                + metrics["avg_mem_efficiency"]
                + metrics["avg_time_efficiency"]
            )
            sort_col = "efficiency_sum"
        else:
            sort_col = sort_by
        
        # Sort and limit results
        metrics_sorted = metrics.sort_values(sort_col, ascending=True).head(12)
        
        # Ensure display column exists
        if "process_display" not in metrics_sorted.columns:
            metrics_sorted["process_display"] = metrics_sorted["process_name"]
        
        # Ensure all std columns exist
        std_cols = [
            "std_cpu_efficiency", "std_mem_efficiency", "std_time_efficiency",
            "std_cpus_used", "std_cpus_requested",
            "std_mem_used_gb", "std_mem_requested_gb",
            "std_runtime_hours", "std_time_requested_hours"
        ]
        for col in std_cols:
            if col not in metrics_sorted.columns:
                metrics_sorted[col] = np.nan
        
        return metrics_sorted
    
    def _melt_metrics(metrics_sorted):
        """Reshape data for plotting."""
        id_vars = [
            "process", "process_name", "process_display", "total_runtime_hours", "n_tasks"
        ]
        # Add all avg and std columns as id_vars
        for metric_name, config in METRIC_CONFIG.items():
            for resource_type in ["used", "requested"]:
                col = config[resource_type]
                id_vars.extend([col, f"std_{col.replace('avg_', '')}"])
        
        # Add efficiency std columns
        id_vars.extend(["std_cpu_efficiency", "std_mem_efficiency", "std_time_efficiency"])
        
        return metrics_sorted.melt(
            id_vars=id_vars,
            value_vars=list(METRIC_CONFIG.keys()),
            var_name="metric",
            value_name="efficiency",
        )
    
    def _add_efficiency_bars(fig, df_melt):
        """Add efficiency bar traces to figure."""
        for metric_name, df_metric in df_melt.groupby("metric"):
            config = METRIC_CONFIG[metric_name]
            used_col = config["used"]
            req_col = config["requested"]
            used_std_col = f"std_{used_col.replace('avg_', '')}"
            req_std_col = f"std_{req_col.replace('avg_', '')}"
            eff_std_col = f"std_{metric_name.replace('avg_', '')}"
            unit = config["unit"]
            
            fig.add_trace(
                go.Bar(
                    x=df_metric["process"],
                    y=df_metric["efficiency"],
                    name=metric_name.replace("avg_", "").replace("_", " ").title(),
                    hovertext=df_metric["process_display"],
                    customdata=df_metric[[
                        used_col, used_std_col, req_col, req_std_col, eff_std_col, "n_tasks"
                    ]].values,
                    error_y=dict(
                        type="data",
                        array=df_metric[eff_std_col],
                        visible=True,
                    ),
                    hovertemplate=(
                        "<b>%{hovertext}</b><br>"
                        f"Efficiency: %{{y:.1f}}% ± %{{customdata[4]:.1f}}%<br>"
                        f"Used: %{{customdata[0]:.2f}} ± %{{customdata[1]:.2f}} {unit} / "
                        f"Requested: %{{customdata[2]:.2f}} ± %{{customdata[3]:.2f}} {unit}<br>"
                        "Tasks: %{customdata[5]:.0f}"
                        "<extra></extra>"
                    ),
                )
            )
    
    def _add_runtime_line(fig, metrics_sorted):
        """Add runtime line trace to figure."""
        fig.add_trace(
            go.Scatter(
                x=metrics_sorted["process"],
                y=metrics_sorted["total_runtime_hours"],
                name="Total Runtime (h)",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="black", width=2, dash="dot"),
                marker=dict(size=6),
                hovertext=metrics_sorted["process_name"],
                hovertemplate="<b>%{hovertext}</b><br>Total Runtime: %{y:.1f} h<extra></extra>",
            )
        )
    
    def _configure_layout(fig, metrics_sorted, sort_by):
        """Configure figure layout."""
        fig.update_layout(
            title=f"Process Efficiency & Runtime Overview (sorted by {sort_by})",
            xaxis_title="Process",
            yaxis=dict(title="Efficiency (%)"),
            yaxis2=dict(
                title="Total Runtime (h)",
                overlaying="y",
                side="right",
            ),
            barmode="group",
            plot_bgcolor="white",
            paper_bgcolor="white",
            autosize=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1.0,
                xanchor="left",
                x=1.2,
                font=dict(size=12),
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#ccc",
                borderwidth=1,
            ),
        )
        
        # Set x-axis labels
        label_map = dict(zip(metrics_sorted["process"], metrics_sorted["process_display"]))
        fig.update_xaxes(
            tickmode="array",
            tickvals=list(label_map.keys()),
            ticktext=list(label_map.values()),
            automargin=True,
        )
    
    # Main execution flow
    metrics_sorted = _prepare_data(metrics, sort_by)
    df_melt = _melt_metrics(metrics_sorted)
    
    fig = go.Figure()
    _add_efficiency_bars(fig, df_melt)
    _add_runtime_line(fig, metrics_sorted)
    _configure_layout(fig, metrics_sorted, sort_by)
    
    return fig

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from IPython.display import display, clear_output
from ipywidgets import interact, Dropdown, FloatSlider, IntSlider


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

    # Default weights
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

    def _normalize(series):
        series = pd.Series(series).fillna(0)
        max_val = series.max()
        if max_val <= 0 or pd.isna(max_val):
            return pd.Series(np.zeros(len(series)), index=series.index)
        return series / max_val

    def _safe_cv(mean, std):
        mean = pd.Series(mean).replace(0, np.nan)
        std = pd.Series(std).fillna(0)
        return std / mean

    def _normalize_weight_group(weights):
        total = sum(weights.values())
        if total <= 0:
            n = len(weights)
            return {k: 1 / n for k in weights}
        return {k: v / total for k, v in weights.items()}

    impact_weights = _normalize_weight_group(impact_weights)
    inefficiency_weights = _normalize_weight_group(inefficiency_weights)
    variability_weights = _normalize_weight_group(variability_weights)

    # ---- Impact ----
    cpu_cost = metrics["total_runtime_hours"] * metrics["avg_cpus_used"]
    mem_cost = metrics["total_runtime_hours"] * metrics["avg_mem_used_gb"]
    time_cost = metrics["total_runtime_hours"]

    cpu_cost_n = _normalize(cpu_cost)
    mem_cost_n = _normalize(mem_cost)
    time_cost_n = _normalize(time_cost)

    impact = (
        impact_weights["cpu_cost"] * cpu_cost_n +
        impact_weights["mem_cost"] * mem_cost_n +
        impact_weights["time_cost"] * time_cost_n
    )

    # ---- Inefficiency ----
    cpu_ineff = 1 - metrics["avg_cpu_efficiency"] / 100
    mem_ineff = 1 - metrics["avg_mem_efficiency"] / 100
    time_ineff = 1 - metrics["avg_time_efficiency"] / 100

    inefficiency = (
        inefficiency_weights["cpu_ineff"] * cpu_ineff +
        inefficiency_weights["mem_ineff"] * mem_ineff +
        inefficiency_weights["time_ineff"] * time_ineff
    )

    # ---- Variability ----
    runtime_cv = _safe_cv(metrics["avg_runtime_hours"], metrics["std_runtime_hours"])
    cpu_cv = _safe_cv(metrics["avg_cpus_used"], metrics["std_cpus_used"])
    mem_cv = _safe_cv(metrics["avg_mem_used_gb"], metrics["std_mem_used_gb"])

    runtime_cv_n = _normalize(runtime_cv.fillna(0))
    cpu_cv_n = _normalize(cpu_cv.fillna(0))
    mem_cv_n = _normalize(mem_cv.fillna(0))

    variability = (
        variability_weights["runtime_cv"] * runtime_cv_n +
        variability_weights["cpu_cv"] * cpu_cv_n +
        variability_weights["mem_cv"] * mem_cv_n
    )

    # ---- Final priority ----
    priority = impact * (inefficiency + variability)

    metrics["priority"] = priority
    metrics["_impact"] = impact
    metrics["_inefficiency"] = inefficiency
    metrics["_variability"] = variability

    metrics["_cpu_cost"] = cpu_cost
    metrics["_mem_cost"] = mem_cost
    metrics["_time_cost"] = time_cost

    metrics["_cpu_cost_norm"] = cpu_cost_n
    metrics["_mem_cost_norm"] = mem_cost_n
    metrics["_time_cost_norm"] = time_cost_n

    metrics["_cpu_ineff"] = cpu_ineff
    metrics["_mem_ineff"] = mem_ineff
    metrics["_time_ineff"] = time_ineff

    metrics["_runtime_cv"] = runtime_cv
    metrics["_cpu_cv"] = cpu_cv
    metrics["_mem_cv"] = mem_cv

    metrics["_runtime_cv_norm"] = runtime_cv_n
    metrics["_cpu_cv_norm"] = cpu_cv_n
    metrics["_mem_cv_norm"] = mem_cv_n

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
                "CPU cost: %{customdata[3]:.2f} (norm: %{customdata[6]:.2f})<br>"
                "MEM cost: %{customdata[4]:.2f} (norm: %{customdata[7]:.2f})<br>"
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