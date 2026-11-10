# Handoff

## Goal
Analyse raw Nextflow trace files to compare pipeline runs and rank processes by resource-optimisation potential, shown as interactive Plotly dashboards in a Jupyter notebook. Current focus: load traces by pipeline name from `data/` folders instead of hand-written comparison groups.

## State
- **Done and pushed to `origin/main`** (`049dd72`): comparison groups removed.
  - `workflow_helpers.py`: `load_pipeline_traces(pipeline, data_dir)` and `find_trace_files` replace `load_comparison_groups`. The `group` column is gone. `annotate_consistent_processes` and `create_process_scaling_explorer` no longer take group arguments.
  - Notebook `notebooks/Resource Contribution Scaling Analysis.ipynb`: config cell is `PIPELINE = "rnaseq"` plus `DATA_DIR`; the scaling explorer cell has no group dropdown.
  - Traces moved from `data/rnaseq_runs/` to `data/rnaseq/`. README has a note on the folder convention.
- **Committed locally, not pushed**: `d367500` on branch `add-agent-instructions` adds `AGENTS.md` and `CLAUDE.md`. No upstream is set and no PR exists.
- **Verified**: `load_pipeline_traces("rnaseq", "data")` and `create_process_scaling_explorer` run on the two rnaseq traces. The full notebook was not re-run.
- **Stale**: the two notebook cells I rewrote have cleared outputs; other cells still hold old outputs showing a `group` column.
- **Not run**: `/check-setup` failed (`Operation not permitted` on `/Users/nadja/Documents/code_files/coding-agent-setup/scripts/`), so the setup repo's freshness is unknown.

## Decisions
- Folder match is a case-insensitive substring of the pipeline name, as the user asked ("folders in data that contain the name of the pipeline"). Consequence: `"rnaseq"` also matches e.g. `data/rnaseq_ramses`.
- Run labels are the filename stem minus the trailing timestamp, so they changed from "rnaseq test" / "rnaseq test full" to "rnaseq test trace" / "rnaseq test full trace". Falls back to full stems if labels collide.
- Consistency is now judged across all loaded runs of the pipeline.

## Next steps
1. Push the branch: `git push -u origin add-agent-instructions`, then open a PR into `main` or fast-forward merge it (the user asked for the commands and has not run them yet).
2. Re-run the notebook (section 2 onward) so stale outputs show no `group` column. This rewrites outputs, so ask the user first.
3. Ask the user about the TODOs in `AGENTS.md`: `plotly` is not declared in `pyproject.toml`, `dash` is imported by `test.py` but absent from `pyproject.toml` and `uv.lock`, and there is no test or lint setup.

## Traps
- Do not modify or delete anything under `data/`; these are real run traces. Ask first.
- Do not re-resolve `uv.lock` without approval.
- Existing `data/` entries such as `execution_trace_*.txt` sit directly in `data/`, not in a subfolder, so `find_trace_files` ignores them.
- Run Python via `.venv/bin/python` or `uv run`, not system `python3`.
- `test.py` is a plotting prototype, not a test suite.
