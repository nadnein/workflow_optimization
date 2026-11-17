# Plan: from our cluster runs to request formulas

This is the step-by-step plan for the modules that phase 2 selects. Which modules is **not decided yet**; `FASTQC`, `TRIMGALORE` and `PICARD_MARKDUPLICATES` appear in the docs only as worked examples (`module_profiles.md`). `workflow.md` describes the tools; this document says what to do, in which order, and when to stop. Each phase has a **gate**: do not go on until it is met.

Principle: the cluster's own data is primary, the AWS tests only guide. Decide `cpus` first, then memory and runtime as a function of input size at that `cpus` (reasoning and sources: `related_work.md`, and below).

## Phase 1: baseline from the full-size tests

1. Run the full-size tests of rnaseq, ampliseq and scrnaseq on the cluster with
   `trace { enabled = true; raw = true; fields = "process,tag,task_id,name,status,exit,attempt,cpus,memory,time,realtime,%cpu,peak_rss,rchar,wchar,read_bytes,write_bytes" }`.
   Note the pipeline version and the Nextflow/container versions for each run.
2. Put the traces into `data/<pipeline>/`, run `scripts/own_traces.py`, then sections 2 to 4 of the notebook.
3. Fill the own-versus-AWS comparison. Add the config actually used on the cluster (institutional profile) to `module_profiles.md`.

**Gate:** every candidate module has own tasks, requests are known, and its pipeline and tool versions are recorded.

## Phase 2: choose the modules

Take modules that meet several shortlist criteria in our own data (not only AWS), appear in several pipelines, and where the research notes do not rule them out. Decide after phase 1, from the own-data shortlist (the AWS shortlist can suggest candidates beforehand). For each chosen module, write the profile in `module_profiles.md` (current config, how the tool scales, versions).

**Gate:** each chosen module has a profile with a hypothesis, e.g. "memory flat, runtime proportional to input" or "memory set by heap".

## Phase 3: fixed inputs for all experiments

- One tool version and one container per module, one node type, no other jobs competing on the node (or record the node load).
- Inputs: real samples from our data, plus subsets made by taking the first fraction of reads (FASTQ) or of reads of a coordinate-sorted BAM region set (for Picard), at 5, 10, 25, 50 and 100 % of the largest sample. Check that the subset still has the expected structure (paired reads together).
- Record the real input file sizes (bytes, compressed) in a table: `module, sample, fraction, input_bytes`. This is the x value for the formulas; `rchar` is the backup.
- 3 repeats per setting.

## Phase 4: `cpus` experiment (per module)

Fixed input (the largest and one small input), `cpus` in 1, 2, 4, 8, 16 (or up to the node limit), memory generous so it cannot interfere.

Record runtime, `%cpu`, peak RSS. Compute:

- speedup = runtime(1 cpu) / runtime(n)
- efficiency = speedup / n
- memory against `cpus`

**Decision rule:** choose the largest `cpus` whose efficiency is at least the threshold (start with 0.7; HPC guides use 70 to 75 %), unless the tool's own limits are lower (TRIMGALORE: at most `cores + 4` with 8 cores; FASTQC: files times threads per file). If the large and small input curves differ clearly, use a two-level rule (small/large) with one threshold on input size; otherwise a fixed value.
Check `memory against cpus`: if memory grows with `cpus`, memory is evaluated at the chosen `cpus` only.

Sources for the method (efficiency = speedup / cores, about 70 to 75 % as the usual cutoff): [Octopus](https://www.octopus-code.org/documentation/12/tutorial/hpc/scaling), [Sydney Informatics Hub](https://sydney-informatics-hub.github.io/training.gadi.intro/07-Optimisation/index.html), [Alliance Canada](https://docs.alliancecan.ca/wiki/Scalability), [Strathclyde](https://docs.hpc.strath.ac.uk/user-guide/scalability/). The cutoff is a convention, not a standard. Real tools differ a lot in how they scale ([Bowtie2 benchmark](https://www.lesswrong.com/posts/TaSZvZDMMhNbtFtCa/benchmarking-bowtie2-threading)). That `cpus` should be fixed per module rather than follow input size is my reasoning, not a sourced finding; the small-versus-large comparison above is what tests it.

**Gate:** `cpus` is chosen and justified for each module, with the efficiency table saved.

## Phase 5: input-size experiment (per module)

At the chosen `cpus`, run all fractions and samples with 3 repeats. Save runtime, peak RSS and `rchar` per task.

Modules where memory is set by a setting (Picard heap): additionally run a **memory sweep** (for example 2, 4, 8, 16, 32 GB at the 100 % input and the 25 % input) and record failures (exit 137/140), runtime and spill behaviour. The smallest memory that never fails plus a margin is the floor.

**Gate:** at least 5 distinct input sizes over at least one order of magnitude per module, plus 3 repeats; failures logged.

## Phase 6: fit formulas

For each module and each of peak memory and runtime:

1. Look at the log-log scatter (`scripts/plot_scaling.py`). If the relation to input size is weak (published methods gate at a Pearson correlation of 0.3 in Ponder and 0.75 in Lotaru; pick one and state it), use a **flat value** (high percentile of the observed values plus a margin).
2. Otherwise fit `y = a + b * x` (or a power law if log-log is clearly straight) with x = input size in GB.
3. Request = fit plus margin. Start with the 95th to 99th percentile of the residuals added to the fit, a floor (for example 128 MB, as in Ponder) and a cap (node limit).
4. Retry rule: keep `task.attempt` scaling (nf-core default already multiplies by attempt).
5. Write it as a Nextflow closure, for example (numbers are placeholders):
   `memory = { Math.max(floor, a.GB + b.GB * inputGb) * task.attempt }` and `time = { (c + d * inputGb).h * task.attempt }`, with `inputGb` computed from the staged input files (`task.ext` or a helper in the config).
6. Validate: leave one sample out and re-predict; then replay all collected tasks (own + experiment) and count (a) tasks that would have been killed, (b) wasted GB-hours and core-hours versus the current request. Report both. Compare with the Ponder rule and a plain high-percentile flat value as baselines.

**Gate:** zero kills on the held-out samples at the chosen margin (or an agreed small rate), and waste reduced against the current request.

## Phase 7: deploy and monitor

Apply the formulas for the chosen modules in a config profile, rerun the full-size tests with tracing, compare with phase 1 (`workflow.md` step 2 again). Keep the old request as a fallback profile.

## Decisions to take along the way

| Question | Where decided | Default |
|---|---|---|
| Which modules | Phase 2 | open: decide from the own-data shortlist |
| Efficiency threshold for `cpus` | Phase 4 | 0.7 |
| Safety margin | Phase 6 | 95th to 99th percentile of residuals |
| Correlation gate for a formula | Phase 6 | use a formula only if the relation is clear and the input range is wide |

## What this plan does not cover

- Energy use: no source found; measure core-hours instead, and use your cluster's energy accounting if available.
- Online adaptation (Sizey, Ponder in a scheduler plugin). Possible later step, `related_work.md`.
- Tools with several inputs of different roles (reference genomes etc.): input size here means the main sample input.
