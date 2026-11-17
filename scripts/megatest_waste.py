#!/usr/bin/env python3
"""
Rank nf-core processes by over-requested resources (writes one ranking by CPU waste and one by memory waste, each averaged per pipeline first), using the public AWS test
results in s3://nf-core-awsmegatests/<pipeline>/results-<sha>/pipeline_info/.
Small results-test-* runs are skipped unless --include-test is given.
Only runs whose Nextflow command (shown in the execution report) used a full-size test profile are kept: --profile
(default `test_full`) is matched as a substring of the `-profile` value, so `test_full_aws`, `arm,test_full` and
`test_full,docker` all count. Runs with no readable command cannot be verified and are skipped. `--profile ''` turns the
check off. The command and profile are kept in the `command` and `profile` columns (arm etc. stay visible there).
Runs before --since (default 2023-01-01) and runs without a timestamp in their file name are skipped.

Requested resources (cpus, memory, time) are not in the default trace file,
but they are embedded in execution_report_*.html, so that is parsed first.
Nothing raw is stored: the tiny execution_trace_*.txt is read first to check
that a run has usage data (peak_rss, %cpu), only then is the report fetched,
reduced in memory to the columns needed, and discarded. Runs without usage
data are skipped entirely.

Usage:
  uv run python scripts/megatest_waste.py --all                      # every pipeline in the bucket
  uv run python scripts/megatest_waste.py rnaseq sarek --out megatest_summary
  uv run python scripts/megatest_waste.py --all --reuse              # rebuild the summaries from the saved tasks parquet, no S3 access
  uv run python scripts/megatest_waste.py rnaseq --latest 3          # 3 newest result dirs per pipeline
"""
import argparse, html, io, json, re, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3, pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

BUCKET = "nf-core-awsmegatests"
s3 = boto3.client("s3", region_name="eu-west-1", config=Config(signature_version=UNSIGNED, max_pool_connections=32))

# columns kept from the execution report; everything else (scripts, env, paths, ...) is dropped
KEEP = ["process", "status", "exit", "attempt", "cpus", "memory", "time",
        "realtime", "%cpu", "peak_rss", "read_bytes", "write_bytes", "rchar", "wchar", "container", "tag"]
# rchar/wchar count all bytes passed through read/write calls (incl. page cache), read_bytes only what came from storage
NUMERIC = [c for c in KEEP if c not in ("process", "status", "container", "tag")]


def ls(prefix, delimiter=True):
    kw = dict(Bucket=BUCKET, Prefix=prefix)
    if delimiter:
        kw["Delimiter"] = "/"
    for page in s3.get_paginator("list_objects_v2").paginate(**kw):
        yield from page.get("CommonPrefixes", []) if delimiter else page.get("Contents", [])


def all_pipelines():
    """Top-level bucket folders that contain results-* directories."""
    tops = [p["Prefix"].rstrip("/") for p in ls("") if re.match(r"\w", p["Prefix"])]

    def has_results(t):
        return any("/results-" in x["Prefix"] for x in ls(f"{t}/"))

    with ThreadPoolExecutor(16) as ex:
        return [t for t, ok in zip(tops, ex.map(has_results, tops)) if ok]


def _read(obj):
    return s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode(errors="ignore")


def num(s):
    return pd.to_numeric(s.replace({"-": None, "": None}), errors="coerce")


def has_usage(df):
    # presence check only: trace files hold human-formatted values ("4.9 MB"), reports raw numbers
    return all(c in df and (~df[c].isin(["-", ""]) & df[c].notna()).any() for c in ("peak_rss", "%cpu"))


def parse_report(text):
    """Extract the trace records embedded in a Nextflow execution report."""
    m = re.search(r"window\.data\s*=\s*", text)
    if not m:
        return None
    # the report is a JS literal: turn JS-only escapes (\') into valid JSON, pairwise so \\' stays intact
    text = re.sub(r"\\(.)", lambda e: "'" if e.group(1) == "'" else e.group(0), text[m.end():], flags=re.S)
    data, _ = json.JSONDecoder().raw_decode(text)
    return pd.DataFrame(data.get("trace", []))


def _tool(name):
    # trim-galore / trimgalore / Trim_Galore are the same tool: compare without case, hyphens and underscores
    return re.sub(r"[-_]", "", name.lower())


def _version(v):
    return v.strip().strip("'\"")  # some versions files quote values: '1.21'


def parse_versions(text):
    """Parse nf_core_*_software_mqc_versions.yml -> ({module: 'tool=ver, ...'}, pipeline version)."""
    mods, cur, pipe = {}, None, None
    for line in text.splitlines():
        if not line.strip():
            continue
        key, _, val = line.strip().partition(": ")
        if not line.startswith(" "):
            cur = line.strip().rstrip(":")
            mods.setdefault(cur, {})
        elif cur == "Workflow":
            pipe = val if key.startswith("nf-core/") else pipe
        else:
            mods[cur][_tool(key)] = _version(val)
    mods.pop("Workflow", None)
    return {m: ", ".join(f"{t}={v}" for t, v in sorted(d.items())) for m, d in mods.items() if d}, pipe


def container_label(container):
    """Fallback tool version from a container name like 'trim-galore:0.6.7--hdfd78af_0'."""
    m = re.search(r"([^/:]+):(\d+(?:\.\d+)+)", str(container))
    return f"{_tool(m.group(1))}={m.group(2)}" if m else "unknown"


def parse_command(text):
    """Nextflow command and its -profile value from an execution report (None, None if not found)."""
    m = re.search(r'<pre class="nfcommand"><code>(.*?)</code>', text, re.S)
    if not m:
        return None, None
    cmd = html.unescape(m.group(1)).strip()
    p = re.search(r"-profile\s+(\S+)", cmd)
    return cmd, (p.group(1).strip("'\"") if p else None)


def collect_pipeline(pipeline, latest=None, include_test=False, since=None, profile="test_full"):
    """Reduced task table for all usable runs of one pipeline (nothing written to disk)."""
    # results-test-<sha>/ are the small `-profile test` runs; everything else is treated as full-size
    dirs = [p["Prefix"] for p in ls(f"{pipeline}/") if "/results-" in p["Prefix"]
            and (include_test or "/results-test-" not in p["Prefix"])]
    # order by newest object in pipeline_info, so --latest is meaningful
    found = []
    for d in dirs:
        objs = list(ls(f"{d}pipeline_info/", delimiter=False))
        if objs:
            found.append((max(o["LastModified"] for o in objs), d, objs))
    found.sort(reverse=True)

    frames = []
    skipped_profile = 0
    for _, d, objs in found[:latest] if latest else found:
        by_name = {o["Key"].rsplit("/", 1)[-1]: o for o in objs}
        ver_obj = next((o for n, o in by_name.items() if n.endswith("software_mqc_versions.yml")), None)
        versions, pipe_version = parse_versions(_read(ver_obj)) if ver_obj else ({}, None)
        for name, o in by_name.items():
            if not name.startswith("execution_trace"):
                continue
            report = by_name.get(name.replace("execution_trace", "execution_report").replace(".txt", ".html"))
            date = re.search(r"_(\d{4}-\d\d-\d\d)_", name)  # older runs may have no timestamp in the file name
            run_date = date.group(1) if date else None
            if since and (run_date is None or run_date < since):
                continue  # too old, or undated and therefore not verifiable; skipped before any big download
            try:
                trace = pd.read_csv(io.StringIO(_read(o)), sep="\t", dtype=str)
                if report is None or not has_usage(trace):
                    continue  # the 30 KB trace says this run is unusable, so the big report is never fetched
                text = _read(report)
                command, run_profile = parse_command(text)
                if profile and profile.lower() not in (run_profile or "").lower():
                    skipped_profile += 1  # not a full-size test, or the command is missing and cannot be verified
                    continue
                df = parse_report(text)
                if df is None or df.empty or not has_usage(df):
                    continue
                df = df.reindex(columns=KEEP)
                df[NUMERIC] = df[NUMERIC].apply(num)
            except Exception as e:  # one malformed file must not abort the whole run
                print(f"[warn] skipping {o['Key']}: {type(e).__name__}: {e}", file=sys.stderr)
                continue
            df["pipeline"] = pipeline
            df["command"] = command
            df["profile"] = run_profile
            df["run"] = d.split("/")[1]
            df["run_date"] = run_date
            df["pipeline_version"] = pipe_version
            # module name without the subworkflow path, e.g. NFCORE_RNASEQ:...:STAR_ALIGN -> STAR_ALIGN
            df["module"] = df["process"].astype(str).str.split(":").str[-1]
            # tool versions: from the run's versions file if present, else from the task's container name
            df["tool_versions"] = df["module"].map(versions).fillna(df["container"].map(container_label))
            frames.append(df)
    print(f"{pipeline}: {len(found)} result dirs, {len(frames)} usable reports"
          f"{f', {skipped_profile} skipped (profile does not contain {profile!r})' if profile else ''}", file=sys.stderr)
    return frames


SORT_COLUMNS = {"cpu": "cpu_waste_per_task_h", "memory": "mem_waste_per_task_gbh"}


def summarise(df, keys=("module",), sort="cpu", min_tasks=0):
    """Waste statistics per group of tasks (default: one row per module, pooled over all runs), ranked by CPU or memory waste; groups with fewer than min_tasks tasks are listed after all others."""
    keys = list(keys)
    ok = df[df["status"].isin(["COMPLETED", "CACHED"])].dropna(subset=["peak_rss", "%cpu", "realtime"]).copy()
    h = ok["realtime"] / 3.6e6  # ms -> h
    ok["mem_req_gbh"] = ok["memory"] / 1e9 * h
    # waste = requested minus actually used, integrated over the task's runtime (never negative)
    ok["mem_waste_gbh"] = (ok["memory"] - ok["peak_rss"]).clip(lower=0) / 1e9 * h
    ok["cpu_req_ch"] = ok["cpus"] * h
    ok["cpu_waste_ch"] = (ok["cpus"] - ok["%cpu"] / 100).clip(lower=0) * h
    g = ok.groupby(keys)
    out = pd.DataFrame({
        "pipelines": g["pipeline"].nunique(),
        "runs": g["run"].nunique(),
        "versions": g["tool_versions"].nunique(),
        "tasks": g.size(),
        "latest_run": g["run_date"].max(),
        "req_mem_gb_median": g["memory"].median() / 1e9,
        "peak_rss_gb_p95": g["peak_rss"].quantile(0.95) / 1e9,
        "mem_eff_median": (ok["peak_rss"] / ok["memory"]).groupby([ok[k] for k in keys]).median(),
        "cpu_eff_median": (ok["%cpu"] / 100 / ok["cpus"]).groupby([ok[k] for k in keys]).median(),
        "mem_waste_gbh": g["mem_waste_gbh"].sum(),
        "cpu_waste_core_h": g["cpu_waste_ch"].sum(),
        "cpu_req_core_h": g["cpu_req_ch"].sum(),
    })
    # pipeline-balanced columns: statistics per pipeline first, then a plain mean over pipelines, so a module
    # is not ranked higher just because AWS ran its pipelines more often or it has more tasks per run
    pk = keys if "pipeline" in keys else keys + ["pipeline"]
    pp = ok.groupby(pk).agg(tasks=("run", "size"), cw=("cpu_waste_ch", "sum"), cr=("cpu_req_ch", "sum"),
                            mw=("mem_waste_gbh", "sum"), mr=("mem_req_gbh", "sum"))
    pp["cpu_waste_per_task_h"] = pp["cw"] / pp["tasks"]
    pp["mem_waste_per_task_gbh"] = pp["mw"] / pp["tasks"]
    pp["cpu_waste_frac"] = pp["cw"] / pp["cr"].where(pp["cr"] > 0)
    pp["mem_waste_frac"] = pp["mw"] / pp["mr"].where(pp["mr"] > 0)
    balanced = ["cpu_waste_per_task_h", "mem_waste_per_task_gbh", "cpu_waste_frac", "mem_waste_frac"]
    out = out.join(pp[balanced].groupby(level=keys).mean())
    front = ["pipelines", "runs", "versions", "tasks", "latest_run"] + balanced
    out = out[front + [c for c in out.columns if c not in front]]
    # exit codes 137/140/143 = killed (typically out-of-memory or walltime), counted from all tasks incl. failed ones
    killed = df[df["exit"].isin([137, 140, 143])]
    out["oom_or_kill_retries"] = killed.groupby(keys).size().reindex(out.index).fillna(0).astype(int)
    # thinly supported groups (few tasks) must not top the ranking on the strength of one or two tasks
    out["ranked"] = out["tasks"] >= min_tasks
    return out.sort_values(["ranked", SORT_COLUMNS[sort]], ascending=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipelines", nargs="*")
    ap.add_argument("--all", action="store_true", help="use every pipeline in the bucket")
    ap.add_argument("--out", default="megatest_summary", type=Path, help="where the summary files are written")
    ap.add_argument("--latest", type=int, default=None)
    ap.add_argument("--since", default="2023-01-01", help="skip runs before this date (YYYY-MM-DD) and undated runs; '' = keep all")
    ap.add_argument("--min-tasks", type=int, default=10, help="modules with fewer tasks are listed after all others in the rankings")
    ap.add_argument("--reuse", action="store_true", help="skip S3 and rebuild the summaries from the matching tasks parquet in --out (same other options as the original run)")
    ap.add_argument("--profile", default="test_full", help="keep only runs whose -profile contains this text (default test_full); '' = no check")
    ap.add_argument("--include-test", action="store_true", help="also use the small results-test-* runs")
    a = ap.parse_args()
    # file names carry the settings that shaped the data, so outputs of different runs never overwrite each other
    scope = "all" if a.all else "-".join(sorted(a.pipelines))
    tag = "__".join(filter(None, [scope, f"since-{a.since or 'any'}", f"latest-{a.latest}" if a.latest else "",
                                  "with-test" if a.include_test else "", f"profile-{a.profile}" if a.profile else ""]))
    tasks_file = a.out / f"tasks__{tag}.parquet"
    if a.reuse:
        df = pd.read_parquet(tasks_file)
    else:
        pipelines = all_pipelines() if a.all else a.pipelines
        if not pipelines:
            ap.error("give pipeline names, --all or --reuse")

        # pipelines are processed in parallel (network-bound); each returns reduced task tables, then all are pooled
        with ThreadPoolExecutor(8) as ex:
            results = ex.map(lambda p: collect_pipeline(p, a.latest, a.include_test, a.since, a.profile), pipelines)
            frames = [f for fs in results for f in fs]
        if not frames:
            sys.exit("No execution reports with usage data found.")
        df = pd.concat(frames, ignore_index=True)
        a.out.mkdir(parents=True, exist_ok=True)
        df.to_parquet(tasks_file)
    # one ranking by CPU waste and one by memory waste
    for sort in SORT_COLUMNS:
        summary = summarise(df, sort=sort, min_tasks=a.min_tasks)
        summary.to_csv(a.out / f"waste_by_module__{tag}__min-tasks-{a.min_tasks}__sort-{sort}.csv")
        print(f"\nTop 10 by {sort} waste per task, averaged over pipelines ({SORT_COLUMNS[sort]}):")
        with pd.option_context("display.width", 200, "display.max_columns", 20):
            print(summary.round(2).head(10))
    print(f"\nwritten to {a.out}/ with tag {tag}")


if __name__ == "__main__":
    main()
