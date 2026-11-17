# Nextflow Pipeline Resource Analysis

Find the processes of the Nextflow pipelines we use where better resource requests pay off, and derive request formulas that depend on input size. Our own cluster traces are the main data; the public nf-core AWS test results are additional guidance. The notebook `notebooks/Resource Optimization Workflow.ipynb` walks through the steps:

- **Data** — our traces (`data/<pipeline>/`) and the AWS test tables
- **Shortlist** — four criteria: failures/retries, long runtime or many tasks, inefficient use of requests, resources correlating with input data
- **Look closer** — resource use against input size per module
- **Detailed views of our runs** — optimization priority ranking, burden per process, scaling across runs

## Setup

This project uses **uv** for reproducible environments.

```bash
git clone <repo-url>
cd workflow_optimization
uv sync
```

## Run notebook (VS Code recommended)

```bash
uv run jupyter notebook
```

Trace files are discovered automatically: put the `*.txt` traces in a folder under `data/` whose name contains the pipeline name (e.g. `data/rnaseq/`) and set `PIPELINES` (and `VIEW_PIPELINE`) in the first cell of the notebook.

Open:
```
notebooks/Resource Optimization Workflow.ipynb
```

Select the `.venv` kernel if prompted.

## Required Nextflow trace config

Add this to your `nextflow.config` to generate compatible trace files:

```groovy
trace {
  enabled = true
  raw = true
  fields = "process,tag,task_id,name,status,exit,attempt,cpus,memory,time,realtime,%cpu,peak_rss,rchar,wchar,read_bytes,write_bytes"
}
```

**Key fields:**
- `cpus`, `memory`, `time` — requested resources  
- `%cpu`, `peak_rss`, `realtime` — actual usage
- `rchar`, `wchar` — bytes read/written through read/write calls (proxy for the input size; `read_bytes`/`write_bytes` miss data served from the OS cache)
- `exit`, `attempt`, `tag` — failures and retries, and the sample a task belongs to

**Repository layout** (run all commands from the repository root):

```
notebooks/   Resource Optimization Workflow.ipynb   entry point
scripts/     command-line steps (AWS tables, own traces, shortlist, plots) and workflow_helpers.py (library for the notebook)
docs/        workflow, experiment plan, module profiles, related work, research notes
data/        our Nextflow traces: data/<pipeline>/*.txt (not modified by the code)
megatest_summary/  own_summary/    generated tables and plots (git-ignored)
```

**Workflow overview:** [`docs/workflow.md`](docs/workflow.md) describes the full path from the pipelines we use to resource formulas (screen with the AWS tests, add the research notes, run experiments on our own data, fit and validate). `scripts/shortlist.py rnaseq ampliseq scrnaseq` produces the prioritised list of modules. The plan with phases and gates is [`docs/experiment_plan.md`](docs/experiment_plan.md), the per-module configuration and tool notes are in [`docs/module_profiles.md`](docs/module_profiles.md), and how published prediction methods work is in [`docs/related_work.md`](docs/related_work.md).

## nf-core megatest waste ranking

`scripts/megatest_waste.py` reads the public nf-core AWS test results (`s3://nf-core-awsmegatests/<pipeline>/results-<sha>/pipeline_info/`) and ranks modules by over-requested CPU and memory. Requested resources are parsed from the embedded `execution_report_*.html`. Small `results-test-*` runs, runs whose `-profile` (read from the report's "Nextflow command") does not contain `--profile` (default `test_full`; substring, so `test_full_aws` and `arm,test_full` are kept, `--profile ''` = no check; the command and profile are saved in the `command`/`profile` columns), runs without usage data (`peak_rss`, `%cpu`) and runs before 2023 (`--since YYYY-MM-DD`, default `2023-01-01`; undated runs are skipped too) are skipped. Raw files are never stored: the small trace file is checked first, the report is read in memory and reduced to the needed columns.

```bash
uv run python scripts/megatest_waste.py --all                       # every pipeline in the bucket
uv run python scripts/megatest_waste.py rnaseq sarek                # selected pipelines
uv run python scripts/megatest_waste.py rnaseq --latest 3           # newest 3 result dirs only
```

One run writes both rankings: one sorted by CPU waste per task (`cpu_waste_per_task_h`), one by memory waste per task (`mem_waste_per_task_gbh`). Both are averaged over pipelines (each pipeline counts equally), so a module is not ranked higher just because its pipelines were run more often on AWS or it has more tasks per run. File names carry the settings, so runs with different options never overwrite each other:

```
megatest_summary/
  tasks__all__since-2023-01-01__profile-test_full.parquet                      reduced per-task rows (columns incl. container, tag, command, profile, rchar, tool_versions, pipeline_version, run_date)
  waste_by_module__all__since-2023-01-01__profile-test_full__min-tasks-10__sort-cpu.csv      ranked by CPU waste
  waste_by_module__all__since-2023-01-01__profile-test_full__min-tasks-10__sort-memory.csv   ranked by memory waste
```

The tag is `all` (or the pipeline names joined by `-`), `since-<date>`, plus `latest-N` / `with-test` if those options are used. `--min-tasks N` (default 10) lists modules with fewer than N tasks after all better-supported ones, so a module with one or two heavy tasks does not top the ranking; the `ranked` column shows which side of the threshold a row is on. `--reuse` rebuilds the CSVs from the matching parquet without touching S3 (give it the same options as the original run).

### Columns of `waste_by_module.csv`

One row per module (process name without the subworkflow path, e.g. `STAR_ALIGN`), pooled over all usable runs, sorted by `cpu_waste_per_task_h` or `mem_waste_per_task_gbh` depending on the file. Only `COMPLETED`/`CACHED` tasks with usage data enter the figures.

| Column | Meaning |
|---|---|
| `module` | Process name without the subworkflow path |
| `pipelines` | Number of pipelines the module appears in |
| `runs` | Number of runs it appears in |
| `versions` | Number of different tool-version combinations pooled in this row (1 = all runs used the same versions; see the detail view below) |
| `latest_run` | Date of the newest run (empty if the report file name has no timestamp) |
| `tasks` | Number of tasks counted |
| `cpu_waste_per_task_h` | **Sort key (CPU file).** Mean over pipelines of that pipeline's wasted core-hours per task, so each pipeline counts equally |
| `mem_waste_per_task_gbh` | **Sort key (memory file).** Same for wasted memory, in GB-hours per task |
| `cpu_waste_frac` | Mean over pipelines of wasted ÷ requested core-hours (0.8 = 80 % of requested CPU time unused) |
| `mem_waste_frac` | Same for memory GB-hours |
| `ranked` | `True` if the module has at least `--min-tasks` tasks; `False` rows are listed after all `True` rows |
| `req_mem_gb_median` | Median requested memory per task (GB) |
| `peak_rss_gb_p95` | 95th percentile of peak memory actually used (GB) |
| `mem_eff_median` | Median of `peak_rss / requested memory` (1.0 = fully used; can exceed 1 if usage overshoots the request) |
| `cpu_eff_median` | Median of `%cpu / 100 / requested cpus` (1.0 = all requested cores busy; can exceed 1) |
| `mem_waste_gbh` | Total (not balanced): sum of (requested − peak used) memory × runtime, in GB-hours, never negative per task |
| `cpu_waste_core_h` | Total (not balanced): sum of (requested cpus − cores used) × runtime, in core-hours, never negative per task |
| `cpu_req_core_h` | Sum of requested cpus × runtime, in core-hours (the denominator for `cpu_waste_core_h`) |
| `oom_or_kill_retries` | Tasks that exited with 137/140/143 (killed, typically out-of-memory or walltime) |

### Detail view for selected modules

```bash
uv run python scripts/module_detail.py TRIMGALORE FASTQC
```

`scripts/module_detail.py` reads the newest `megatest_summary/tasks__*.parquet` (or the one given with `--data`; no S3 access) and writes `megatest_summary/detail__<tag>__<MODULES>.csv`: one row per module, pipeline, pipeline version, run, run date and tool-version combination, with the same waste columns as above. The tool versions come from the run's `nf_core_*_software_mqc_versions.yml`; if the run has none, from the container name (`tool=version`); otherwise `unknown`. Old runs may show a pipeline container such as `chipseq=1.2.0` instead of a tool. Input data is not included yet.

### Selecting candidate modules

```bash
uv run python scripts/module_candidates.py                 # defaults below
uv run python scripts/module_candidates.py --min-pipelines 3 --max-cpu-eff 0.4 --min-other 3
```

`scripts/module_candidates.py` reads the newest `tasks__*.parquet` (no S3 access) and writes `megatest_summary/candidates__<tag>__<thresholds>.csv`: one row per module with a metric and a yes/no flag per criterion, `criteria_met` and `candidate`. A module is a `candidate` if it passes criterion 1, has at least `--min-tasks` tasks and meets at least `--min-other` of criteria 2-5.

| Criterion (flag) | Metric | Default threshold |
|---|---|---|
| 1. `multi_pipeline` | `pipelines` | `--min-pipelines 2` |
| 2. `failures` | `failure_rate` (FAILED share of finished tasks) or `retry_rate` (share with attempt > 1); ABORTED tasks are not counted | `--min-fail-rate 0.02` |
| 3. `runtime` | `median_runtime_min` or `total_runtime_h` | `--min-median-runtime-min 10` or `--min-total-runtime-h 100` |
| 4. `inefficient` | `cpu_eff_median`, or `mem_slack_gb` (median requested memory minus 95th percentile of peak use) | `--max-cpu-eff 0.3` or `--min-mem-slack-gb 16` |
| 5. `input_sensitive` | Spearman correlation of peak memory, runtime and CPU time (runtime × `%cpu`, in core-hours) with (a) `rchar` (`corr_rchar_rss`, `corr_rchar_runtime`, `corr_rchar_cpu`) and (b) the sample's real input size from `scripts/input_sizes.py` (`corr_input_*`, only where a samplesheet could be resolved). Computed per pipeline (>= `--min-corr-tasks 20` tasks and >= 3 distinct input values) and averaged over pipelines; the flag is set if any of them is >= `--min-corr` | `--min-corr 0.5` |

Memory and walltime efficiency (`mem_eff_median`, `time_eff_median`) are in the table but not flagged: nearly every module uses less than 10 % of the default request, so they do not separate modules. `rss_p95_over_p50` and `runtime_p95_over_p50` show how much a module's use varies. `input_gb_min`, `input_gb_max` and `input_distinct_sizes` show how wide the input sizes are that the correlations rest on: a strong correlation over a narrow range or few sizes proves little. Without `input_sizes__<tag>.parquet` only the `rchar` columns are filled, and the output file name says `inputsizes-no`.

### Input size per sample and the `rchar` proxy

```bash
uv run python scripts/input_sizes.py       # real input size per sample (web/S3 lookups, no downloads)
uv run python scripts/rchar_vs_input.py    # how well does rchar follow the real input size?
```

`scripts/input_sizes.py` reads each run's `params_*.json`, takes the `input` samplesheet (web or S3), and looks up the size of every listed file with a HEAD request. Sizes are summed per sample, so they join to the task tag (`tasks.tag == sample`). It writes `megatest_summary/input_sizes__<tag>.parquet` (columns `pipeline`, `run`, `sample`, `input_bytes`, `n_files`, `n_sized`). Only about two thirds of the runs can be resolved: runs with no params file, no samplesheet on the web or S3, or only relative file names get no sizes, and nothing is guessed. `ftp://` and `gs://` files cannot be sized. Progress is cached in `megatest_summary/input_sizes_cache.json`, so an interrupted run (e.g. lost network) continues where it stopped; a sizes file is never written when no file could be sized.

`scripts/rchar_vs_input.py` compares `rchar` (bytes a task read through read calls, available for nearly all tasks; `read_bytes` is empty for more than half of them because it ignores the OS cache) with the real input size, and writes two files: `rchar_vs_input__<tag>.csv` (per module: Spearman correlation and the median `rchar` ÷ input ratio) and `rchar_vs_input_by_pipeline__<tag>.csv` (the same per module and pipeline, with the 10th/90th percentile of the ratio). Read the ratio per pipeline: about 1 means the task reads its input once, much larger means fixed overhead on tiny inputs or large references and indexes, below 1 means the samplesheet lists files the task does not read.

### Plots

```bash
uv run python scripts/plot_scaling.py PICARD_MARKDUPLICATES TRIMGALORE FASTQC     # x axis = real input size
uv run python scripts/plot_scaling.py TRIMGALORE --x rchar                        # x axis = rchar (covers more tasks)
```

`scripts/plot_scaling.py` writes `megatest_summary/plots/scaling__<tag>__<x>__<MODULE>.html` (interactive Plotly) and a `.csv` with the plotted points: peak memory, runtime and CPU time against input size, one task per dot, log-log axes, with a power-law fit. Panel titles show rho (average per-pipeline Spearman correlation) and the exponent b of y = a·x^b (near 0: flat, near 1: proportional). The three pipelines with most tasks are coloured, all others grey. With `--x input` only tasks whose sample has a resolved input size are shown, which can be a fraction of the module's tasks.

### Module table for selected pipelines

```bash
uv run python scripts/pipeline_view.py rnaseq ampliseq scrnaseq smrnaseq detaxizer
```

`scripts/pipeline_view.py` (no S3 access) writes `megatest_summary/pipeline_modules__<tag>__<pipelines>.csv`: one row per pipeline and module, sorted by total runtime within each pipeline. It shows the size of the module in that pipeline (tasks, runs, runtime), the waste totals, request against actual use (`req_mem_gb_median`, `peak_rss_gb_p95`, `cpu_eff_median`), `failure_rate`, the Spearman correlations of peak memory and runtime with `rchar` and with the real input size (empty with fewer than 10 tasks or 3 distinct values), the input range those correlations rest on, and `pipelines_total` (how many pipelines use the module). Use it to see which modules of the pipelines you run are worth a closer look, and `scripts/module_detail.py` / `scripts/plot_scaling.py` for the details.

### Shortlist for the pipelines we use

```bash
uv run python scripts/shortlist.py rnaseq ampliseq scrnaseq
```

`scripts/shortlist.py` applies four criteria (failures/retries, long runtime or many tasks, inefficient use of requests, resources correlating with input data) to every module of the chosen pipelines, joins the research notes from `docs/tool_scaling_evidence.csv`, and writes `megatest_summary/shortlist__<tag>__<pipelines>.csv`, ordered by criteria met, with `reasons` and a rule-of-thumb `next_step`. Thresholds are command-line options. See `docs/workflow.md` for how to read it and what to do next.

### Background on tool resource behaviour

`docs/tool_resource_scaling.md` collects what the tools' own documentation says about what drives their memory and runtime, with sources and an evidence level for every statement, plus modules worth researching next.
