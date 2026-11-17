# Handoff

## Goal
Use the public nf-core AWS megatest results (`s3://nf-core-awsmegatests`) to find modules whose CPU/memory/runtime follow input size, and end with a usable resource formula for 2-3 modules. Only results with usage data (`peak_rss`, `%cpu`) from 2023 on matter, and raw data is not kept.

## State
Nothing from this work is committed. `git status`: modified `.gitignore`, `README.md`, `AGENTS.md`, `HANDOFF.md`, `pyproject.toml` (boto3, pyarrow, plotly), `uv.lock`; deleted `notebooks/Resource Contribution Scaling Analysis.ipynb` and `test.py` (authorised, in git history); new `notebooks/Resource Optimization Workflow.ipynb`, `docs/`, and the scripts below.

Entry point: **`notebooks/Resource Optimization Workflow.ipynb`** (follows `docs/workflow.md`; executed end to end without errors in a scratch copy, widget output and plots not looked at in a browser). `scripts/workflow_helpers.py` is kept because section 5 of the notebook (priority ranking, burden per process, scaling across runs, carried over from the old notebook) uses it. `main.py` is an unrelated unused placeholder.

Scripts (`scripts/`, documented in `README.md`; outputs in git-ignored `megatest_summary/` = AWS tests and `own_summary/` = our traces):
1. `scripts/megatest_waste.py --all`: per-task table from the nf-core AWS tests (343,620 tasks, 91 pipelines, runs from 2023) and two module rankings. `scripts/input_sizes.py`: real input size per sample (435 of 684 runs resolved). `scripts/rchar_vs_input.py`: `rchar` tracks real input size (median Spearman 0.68).
2. `scripts/own_traces.py PIPELINE...`: our traces from `data/<pipeline>/` to the same per-task table (failed tasks kept; needs raw traces). Tested on the two rnaseq traces in `data/rnaseq/`; ampliseq and scrnaseq traces do not exist yet.
3. `scripts/pipeline_view.py` (metrics), `scripts/shortlist.py` (four criteria + research notes `docs/tool_scaling_evidence.csv`; no module dropped for few tasks, marked `low data`), `scripts/module_candidates.py`, `scripts/module_detail.py`, `scripts/plot_scaling.py` (`--data`/`--out` select AWS or own table).
4. `docs/tool_resource_scaling.md`: documented scaling behaviour of 19 tools with sources and evidence levels.

## Decisions
- `results-test-*` runs, runs before 2023, undated runs and runs without usage data are skipped. Everything else counts as full-size (this still includes small ad-hoc runs).
- Rankings use waste per task averaged over pipelines, not totals, so AWS testing frequency does not drive the order. Modules with fewer than 10 tasks sort last.
- Tool versions are normalised (lowercase, no `-`/`_`, no quotes) so equal versions pool; the main CSV is one row per module, version detail lives in `scripts/module_detail.py`.
- `rchar` replaces `read_bytes` as the input proxy (filled for 99.9 % of tasks vs 44 %).
- Memory/walltime efficiency are not flagged (nearly all modules use under 10 % of the request).

## Next steps
1. User runs the full-size tests of rnaseq, ampliseq and scrnaseq on the cluster with `trace.raw = true` and the fields in the README, puts the traces into `data/<pipeline>/` (not into the chat: traces are sensitive), then run `scripts/own_traces.py` and the notebook sections 2-4. Own runs are the main evidence, AWS the guidance.
2. Open the notebook and the plots in `megatest_summary/plots/` in a browser and fix layout problems.
3. Pick 2-3 modules (suggested: `PICARD_MARKDUPLICATES`, `TRIMGALORE`, `FASTQC`; `MIRDEEP2` in smrnaseq has the largest saving), run size experiments where the input range is narrow, then build the formula fitting (step 6 of `docs/workflow.md`, not built). Commit the work.

## Traps
- Run Python via `uv run`; system `python3` is not available. The sandbox hides the process list (`pgrep`, `ps`, `pkill` fail).
- Do not modify or delete anything under `data/`. Ask before re-resolving `uv.lock`.
- Real input sizes exist for only about 64 % of runs; e.g. PICARD_MARKDUPLICATES has them for 90 of 611 tasks. Use `--x rchar` for full coverage. AWS test inputs span a narrow range, so do not extrapolate far.
- A full `scripts/megatest_waste.py --all` takes several minutes and a few GB of network traffic; `--reuse` rebuilds the CSVs from the saved parquet. `scripts/input_sizes.py` needs the network; rerun it to continue from its cache.
- zsh aborts a whole `&&` chain when `rm -f` has a glob that matches nothing.
