# Project instructions

## Project overview
- Purpose: analyse raw Nextflow `trace` files (`*.txt`, tab-separated) to compare
  pipeline runs and rank processes by resource-optimisation potential. Metrics are
  CPU core-hours, peak-RSS-weighted GB-hours, runtime hours, and I/O GB, plus
  used-vs-requested efficiency and variability. Output is interactive Plotly
  dashboards in a Jupyter notebook, not a CLI tool.
- Main languages / frameworks: Python 3.13, pandas, numpy, Plotly
  (`graph_objects`, `subplots`), `ipywidgets`/IPython for interactivity.
- Important directories and files:
  - `workflow_helpers.py` — the whole library (~1200 lines): loading, cleaning,
    derived metrics, consistency detection, plotting.
  - `notebooks/Resource Contribution Scaling Analysis.ipynb` — the entry point
    (setup → load → inspect tables → three interactive views).
  - `data/<pipeline>/*.txt` — Nextflow traces; folder name must contain the
    pipeline name (case-insensitive substring match).
  - `test.py` — plotting prototypes (efficiency bars, bubble plot, score plot);
    **not** a test suite.
  - `main.py` — unused `uv init` placeholder.
  - `uv.lock` — resolved dependency lockfile.

## Environment and setup
- Supported runtime versions: `requires-python = ">=3.13"`; `.python-version`
  pins `3.13`; the existing `.venv` was built with uv 0.10.3 on CPython 3.13.11.
- Dependency installation: `uv sync` (README). Package manager is **uv**;
  `uv.lock` is committed and `.venv/` is git-ignored.
- Configuration / environment variables: no `.env` usage and no environment
  variables are read in code. The only configuration is in the notebook cell:
  `PIPELINE = "rnaseq"` and `DATA_DIR = PROJECT_ROOT / "data"`.
- External services: none. No network calls, no shelling out.
- Declared dependencies in `pyproject.toml`: `ipykernel`, `jupyter`,
  `matplotlib`, `numpy`, `pandas`. Note: `plotly` is imported everywhere but is
  **not** declared in `pyproject.toml` (it is present in `.venv`/`uv.lock`
  transitively); `dash` is imported by `test.py` but appears in neither
  `pyproject.toml` nor `uv.lock`. TODO: ask the user whether these should be
  added as explicit dependencies or the imports removed.

## Development workflow
- Run: `uv run jupyter notebook`, then open
  `notebooks/Resource Contribution Scaling Analysis.ipynb` and select the
  `.venv` kernel. Notebook charts render with `renderer="plotly_mimetype"`.
- Tests: no test framework, no `tests/` directory, no CI config, and `test.py`
  contains no assertions. TODO: ask the user how tests should be run.
- Linting / formatting: no ruff/black/flake8/mypy configuration and no
  `[tool.*]` sections in `pyproject.toml`. TODO: ask the user for the intended
  formatter/linter.
- Build / validation: nothing to build. Minimal smoke check is loading a trace
  in Python (`load_and_preprocess_trace`) or re-running notebook section 2; avoid
  re-running the full notebook unnecessarily.

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
