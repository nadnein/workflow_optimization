# Workflow: from pipelines to resource formulas

Goal: for the pipelines we use, find the processes where better resource requests pay off, and for the promising ones
derive a request formula (depending on input size, or a better fixed value).

Steps marked **built** have a script in `scripts/`; steps marked **manual** or **not built yet** are described so
they can be done by hand. All commands run with `uv run python ...`.

## 0. Prepare the AWS test data (built, only when the data should be refreshed)

```bash
scripts/megatest_waste.py --all      # reduced per-task table + waste rankings from the nf-core AWS tests (several minutes, a few GB of traffic)
scripts/input_sizes.py               # real input size per sample from the samplesheets (needs network; resumes from a cache)
```

Outputs go to the git-ignored `megatest_summary/`. Everything below reads those files and needs no network.
`scripts/rchar_vs_input.py` shows how far `rchar` can stand in for the input size where no samplesheet could be resolved.

## 1. Choose the pipelines (manual)

Take the pipelines used at our institution, most used first. Currently: **rnaseq, ampliseq, scrnaseq** (also used:
smrnaseq, detaxizer). Modules that appear in many pipelines are worth extra attention because one formula serves all of
them (`pipelines_total` in the outputs).

## 2. Screen the modules of those pipelines (built)

```bash
scripts/shortlist.py rnaseq ampliseq scrnaseq
```

Applies four criteria per module within these pipelines, using the AWS tests:

| Criterion | Flag | Default |
|---|---|---|
| High failure or retry rate | `failures` | failed or retried >= 2 % of finished tasks |
| Long runtime or many tasks | `runtime` | median >= 10 min, or >= 10 h in the pipeline, or >= 500 tasks |
| Inefficient use of allocated resources | `inefficient` | CPU efficiency <= 0.3, or >= 16 GB memory slack (median request minus 95th percentile of use) |
| Resources correlate with input data | `input_dependent` | Spearman rho >= 0.5 of peak memory, runtime or CPU time with `rchar` or the real input size |

No module is dropped for having little data: those with fewer than 10 tasks are marked `low data` in the `evidence` column and sorted after better-supported ones. Output:
`megatest_summary/shortlist__<tag>__<pipelines>.csv`, ordered by criteria met, with `reasons`, a rule-of-thumb
`next_step`, all metrics, and the research notes where the module is covered. Thresholds are options (`--help`).

Cautions when reading it: failure rates come from nf-core test runs and may have causes unrelated to resources; small
samples (10-30 tasks) make rates and correlations noisy; correlations over a narrow input range prove little, check
`input_gb_min/max` and `input_distinct_sizes`.

## 3. Look closer at the candidates (built)

```bash
scripts/module_detail.py MODULE...       # every run: pipeline, version, tool versions, waste
scripts/plot_scaling.py MODULE...        # memory, runtime, CPU time against input size (HTML + CSV); add --x rchar for more tasks
scripts/pipeline_view.py rnaseq ...      # the full metrics table behind the shortlist
```

Check that the tool version is consistent, whether the pipelines agree with each other, and whether the relation looks
like a line on the log-log plot or flat.

## 4. Decide per module what to do (manual, guided by `next_step`)

| Finding | Next step |
|---|---|
| Resources correlate with input, input range wide (>= 10 distinct sizes, max/min >= 10) | Fit a request formula (step 6) |
| Resources correlate with input, range narrow | Run an experiment with inputs over a wider range (step 5) |
| Flat resource use, request far above use | Try a smaller fixed request, confirm on more data |
| High failure rate | Find the cause first: exit codes 137/140/143 point to memory or walltime |
| Research says the resource is set by a reference or a setting, not by reads (e.g. STAR memory by genome size, samtools sort by `-m`) | Base the request on that driver, not on input size |

Use the research notes (`docs/tool_resource_scaling.md`, machine-readable `docs/tool_scaling_evidence.csv`) to rule
candidates in or out; modules the notes do not cover are open questions, not negatives.

## 5. Run experiments on our own data (manual)

For modules that need a wider input range or more data:
- Use one fixed tool version and thread count.
- Test at least 5 input sizes over at least one to two orders of magnitude (for example read subsets at 5, 10, 25,
  50 and 100 % of a real sample), 3 repeats each.
- Record a Nextflow trace with these fields, which include everything the analysis uses:
  `process,tag,task_id,name,status,exit,attempt,cpus,memory,time,realtime,%cpu,peak_rss,rchar,wchar,read_bytes,write_bytes`
  (`rchar` is the proxy for input volume; also note the real input file sizes).
- Load the traces with the existing notebook/`scripts/workflow_helpers.py`, or append them to the per-task table in the same
  column layout, so steps 2-3 can be repeated with our own data.

Details for the chosen modules (what to run, in which order, with gates) are in `experiment_plan.md`; the current
configuration and tool characteristics per module are in `module_profiles.md`; how other methods derived their formulas
is in `related_work.md`.

## 6. Fit and validate a formula (not built yet)

Per module: runtime and memory against input size (power law, or linear with intercept; flat plus margin where there is
no relation), validated by leaving one pipeline or run out; choose a safety margin so that 95-99 % of tasks fit; compare
with the current request (waste saved against the risk of out-of-memory kills); write it as a Nextflow closure, for example
`memory = { 2.GB + 0.8.GB * input_gb * task.attempt }`. Suggested first modules: `PICARD_MARKDUPLICATES` (memory and
runtime follow input), `TRIMGALORE` and `FASTQC` (flat memory, runtime follows input).

## 7. Deploy and monitor (manual)

Apply the new requests to the pipeline config, re-run with tracing on, and check the waste and failure rates again
with step 2.

## Limits of the AWS tests

They are nf-core test datasets: relative scaling is more trustworthy than absolute numbers, input ranges are narrow,
some pipelines have few runs (rnaseq: 4), and a module used in one pipeline cannot be cross-checked. Real input sizes
exist for about two thirds of the runs. That is why steps 5-6 use our own data.
