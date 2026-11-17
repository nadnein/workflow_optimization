# Project instructions

## Project overview
- Purpose: find the processes of the Nextflow pipelines used at the institution (rnaseq,
  ampliseq, scrnaseq first) where better resource requests pay off, and derive request
  formulas depending on input size. Primary data: our own cluster traces (`data/<pipeline>/*.txt`);
  guidance: the public nf-core AWS test results. Output: CSV tables, Plotly plots and a
  Jupyter notebook that ties the steps together (`docs/workflow.md`).
- Main languages / frameworks: Python 3.13, pandas, numpy, boto3 (anonymous S3), pyarrow, Plotly
  (`graph_objects`, `subplots`), `ipywidgets`/IPython for interactivity.
- Important directories and files:
  - Layout: `notebooks/`, `scripts/` (all Python code, run from the repo root), `docs/`, `data/`, generated
    `megatest_summary/` and `own_summary/`.
  - `notebooks/Resource Optimization Workflow.ipynb` — the entry point; follows `docs/workflow.md`.
  - `docs/workflow.md` (the workflow), `docs/tool_resource_scaling.md` (research notes with sources),
    `docs/tool_scaling_evidence.csv` (machine-readable version).
  - `scripts/megatest_waste.py` (AWS test tables), `scripts/input_sizes.py`, `scripts/rchar_vs_input.py`, `scripts/own_traces.py`
    (our traces to the same per-task table), `scripts/pipeline_view.py`, `scripts/shortlist.py`, `scripts/module_candidates.py`,
    `scripts/module_detail.py`, `scripts/plot_scaling.py` — scripts, documented in README.md. Outputs go to the
    git-ignored `megatest_summary/` (AWS) and `own_summary/` (our traces).
  - `docs/experiment_plan.md` (phases and gates to formulas), `docs/module_profiles.md` (config and tool
    characteristics per module), `docs/related_work.md` (how other prediction methods work).
  - `scripts/workflow_helpers.py` — library for the detailed views of our own runs (section 5 of the
    notebook): loading, derived metrics, optimization priority, plots.
  - `data/<pipeline>/*.txt` — our Nextflow traces; folder name must contain the
    pipeline name (case-insensitive substring match). Traces should be raw (`trace.raw = true`).
  - `uv.lock` — resolved dependency lockfile.

## Environment and setup
- Supported runtime versions: `requires-python = ">=3.13"`; `.python-version`
  pins `3.13`; the existing `.venv` was built with uv 0.10.3 on CPython 3.13.11.
- Dependency installation: `uv sync` (README). Package manager is **uv**;
  `uv.lock` is committed and `.venv/` is git-ignored.
- Configuration / environment variables: no `.env` usage and no environment
  variables are read in code. Configuration is the first cell of the notebook (`PIPELINES`,
  `VIEW_PIPELINE`, directories) and script command-line options.
- External services: only `scripts/megatest_waste.py` and `scripts/input_sizes.py` use the network, anonymously
  (public S3 bucket `nf-core-awsmegatests`, GitHub raw and public S3 for samplesheet file sizes);
  they download no raw data. `scripts/megatest_waste.py` keeps only runs whose `-profile` contains `test_full`. All other scripts and the notebook work offline on the saved tables.
  Our own traces never leave the machine: `scripts/own_traces.py` reduces them locally.
- Declared dependencies in `pyproject.toml`: `ipykernel`, `jupyter`, `matplotlib`, `numpy`,
  `pandas`, `boto3`, `pyarrow`, `plotly`.

## Development workflow
- Run: `uv run jupyter notebook`, then open
  `notebooks/Resource Optimization Workflow.ipynb` and select the
  `.venv` kernel. Notebook charts render with `renderer="plotly_mimetype"`.
  Scripts: `uv run python <script>.py --help`.
- Tests: no test framework, no `tests/` directory, no CI config. TODO: ask the
  user how tests should be run.
- Linting / formatting: no ruff/black/flake8/mypy configuration and no
  `[tool.*]` sections in `pyproject.toml`. TODO: ask the user for the intended
  formatter/linter.
- Build / validation: nothing to build. Minimal smoke check is loading a trace
  in Python (`load_and_preprocess_trace`) or `scripts/own_traces.py`; a notebook check can be run
  into a scratch copy (`jupyter nbconvert --execute --output-dir <scratch>`) so repo outputs stay untouched.

## Project-specific conventions
- Architecture: pure functions over pandas DataFrames, no classes, no global
  state. Pipeline is `find_trace_files` → `load_pipeline_traces` (returns
  `(run_summary_df, process_df, trace_files, labels)`) → `build_per_process_summary`
  / `annotate_consistent_processes` → `create_*` plot functions returning
  `go.Figure` (or `None` when there is nothing to plot).
- Naming: `snake_case`; helpers private to the module are prefixed `_`
  (e.g. `_normalize_series`, `_safe_cv`, `_run_label`, `_build_process_color_map`).
  Derived columns are suffixed with their unit: `_h`/`_hours` for hours, `_gb`
  for gigabytes, `_pct`/`_efficiency` for percentages; intermediate weight terms
  in `add_optimization_priority_columns` are `_`-prefixed (`_impact`,
  `_inefficiency`, `_runtime_cv_norm`). Columns mirroring Nextflow headers keep
  the original names (`%cpu`, `process`, `peak_rss`, `realtime`, `time`, `cpus`).
- Style: public functions carry docstrings (Args/Returns where non-trivial, some
  with `Example:` blocks), type hints on newer functions (`Dict`/`List`/`Tuple`
  from `typing` in older code, `Path | str` and `X | None` in newer code).
  Source is split into sections by banner comments:
  `# ══════...` with an uppercase title, and step markers `# ── Step 1: ...`.
  Plot style: white `plot_bgcolor`/`paper_bgcolor`, hover templates built from
  `<b>%{...}</b>` strings, process colors from a fixed hex palette in
  `_build_process_color_map`.
- Data formats: traces are TSV with Nextflow semantics — `realtime`/`time` in
  milliseconds, `memory`/`peak_rss`/`read_bytes`/`write_bytes` in bytes, `%cpu`
  as a percentage string possibly containing `%`, thousands separators, or `-`
  for missing values. Only `COMPLETED` and `CACHED` tasks are kept; rows missing
  essential metrics are dropped. Run labels come from the filename stem with the
  trailing `_YYYY-MM-DD_HH-MM-SS` timestamp removed.
- Performance / resource constraints: everything is in-memory pandas groupby
  aggregation over whole trace files; no streaming or caching layer.

## Important constraints
- Files or interfaces that must not change: the public helper names and the
  `load_pipeline_traces` return tuple are documented usage in the notebook and
  README; changing them requires updating both. The required Nextflow `trace`
  fields (listed in `README.md`) define the input contract.
- Expensive operations requiring approval: modifying or deleting anything under
  `data/` (these are real run traces and cannot be regenerated offline),
  re-running notebook cells that rewrite outputs, or re-resolving `uv.lock`.
- Security / privacy considerations: trace files describe internal HPC nodes,
  process names and task ids; treat them as potentially sensitive and do not
  paste trace contents into external services.
