# Module profiles: current configuration and tool characteristics

**The modules are not chosen yet.** `FASTQC`, `TRIMGALORE` and `PICARD_MARKDUPLICATES` are worked examples (they came up in the AWS shortlist and appear in many pipelines), not a decision. The decision is phase 2 of `experiment_plan.md`; then add the chosen modules here using the template at the end.

Evidence levels: **documented** (tool docs or module code, quoted), **observed** (our trace or the AWS tables), **inference** (my reading, not stated anywhere), **not found**. Retrieved 2026-11-17.

## How nf-core sets the requests

Labels come from the module (`label 'process_low'` etc.); the values from the pipeline's `conf/base.config` (nf-core/rnaseq master, no `withName` override for these three):

| Label | cpus | memory | time |
|---|---|---|---|
| process_single | 1 | 6 GB * attempt | 4 h * attempt |
| process_low | 2 * attempt | 12 GB * attempt | 4 h * attempt |
| process_medium | 6 * attempt | 36 GB * attempt | 8 h * attempt |
| process_high | 12 * attempt | 72 GB * attempt | 16 h * attempt |

`errorStrategy = { task.exitStatus in ((130..145) + 104 + (175..177)) ? 'retry' : 'finish' }`. **A retry therefore multiplies cpus, memory and time** by the attempt number. This matters for the formulas: the escalation rule is already `task.attempt`-based and affects cpus too.

Requests seen in our own rnaseq full-size trace (`data/rnaseq`, 8 tasks per module; memory in the trace is in GiB shown as bytes):

| Module | Requested (cpus / memory / time) | Matches label | p95 peak RSS | Median runtime | Median %cpu |
|---|---|---|---|---|---|
| FASTQC | 2 / 12 GiB / 4 h | process_low | 1.5 GB | 8.9 min | 190 |
| TRIMGALORE | 12 / 72 GiB / 16 h | process_high | 1.7 GB | 32 min | 697 |
| PICARD_MARKDUPLICATES | 6 / 36 GiB / 8 h | process_medium | 31.7 GB | 39 min | 102 |

Open point: the current module code labels TRIMGALORE `process_medium`, our run used `process_high`. The cause (older module version, pipeline override or cluster config) is **not known**; check the pipeline version and your institutional config. The module and tool versions of our run are not in the trace.

AWS tests (rnaseq, ampliseq, scrnaseq, `shortlist`): see the metrics in `megatest_summary/shortlist__*.csv`. In the rnaseq AWS runs input per sample was 14 to 19 GB with 8 distinct sizes, a narrow range.

## FASTQC

- **Module code (master)** (documented): `label 'process_low'`, container `fastqc:0.13.0`; passes `--threads ${task.cpus}`; computes `--memory` as `task.memory / task.cpus` in MB, clamped to 100 to 10000.
- **Tool** (documented, FastQC script help text on master): `-t/--threads` "Total number of CPU threads to use. Processing a single file can use up to 4 threads." Default 4, capped at available CPUs. `--memory` is "the base amount of memory, in Megabytes, used to process each file", default 512 MB. The heap is multiplied by `min(files, max(1, threads // 4))`.
- **Version caveat** (inference): older FastQC releases are widely described as one thread per file; the help text above is for the current master. Check the version of your run.
- **Observed** (own trace): 2 cpus gave about 190 % CPU, consistent with 2 files (paired-end) at one thread each. Peak RSS stays near 1.5 GB against a 12 GiB request. AWS tables: memory not correlated with input in ampliseq (rho 0.09 to 0.33), runtime weakly related.
- **Scaling known?** Memory vs read count: **not found** in the docs. Threads vs runtime: depends on the number of input files; with 2 files, more than 2 cpus (or 8 per file in newer versions) may help or not; measure.
- **Likely outcome** (inference): fixed small memory request with a margin, `cpus` equal to the number of files (or a small multiple), runtime from input size.

## TRIMGALORE

- **Module code (master)** (documented): `label 'process_medium'`, container `trim-galore:2.3.0`; `cores = task.cpus - 4` for paired-end, `task.cpus - 3` for single-end, at least 1, at most 8; passed as `--cores`.
- **Tool** (documented, README): `--cores N` runs trimming and gzip in an "N+4 thread model" for FASTQ input; "near-linear speedup up to `--cores 8` for paired-end runs", beyond that gzip output I/O usually becomes the bottleneck. Memory: "controlled by the global `--memory` flag (default `1G`)" (matters for `--clumpify`).
- **Observed** (own trace): 12 cpus with about 697 % CPU, which fits `12 - 4 = 8` cores of work. AWS rnaseq: request 77 GB, p95 use about 1.7 GB (memory slack 76 GB); runtime correlates with input (rho 0.50 to 0.52), memory does not (0.18 to 0.27).
- **Consequence** (inference from module code + README): with this module, requesting more than 12 cpus gives nothing for paired-end data, and any request above `cores + 4` is wasted by construction. Requests below 12 reduce the cores used proportionally.
- **Version caveat:** the README describes v2.2.0 behaviour; older 0.6.x releases (still common in rnaseq releases) may differ. Check which version your run used.
- **Scaling known?** Threads: documented to about 8 cores. Input size vs runtime: not stated, AWS shows a relation. Memory vs input: not stated, flat in AWS.
- **Likely outcome** (inference): flat small memory; `cpus` set from the thread formula; runtime from input size.

## PICARD_MARKDUPLICATES

- **Module code (master)** (documented): `label 'process_medium'`, container `picard:3.5.0`; `avail_mem = 3072` MB if no memory is known, otherwise `task.memory.mega * 0.8`; passed as `-Xmx${avail_mem}M`. The module uses no thread setting.
- **Tool** (documented): Picard "most of the commands are designed to run in 2GB of JVM" (command-line overview). For MarkDuplicates, memory is governed by the Java heap and `SORTING_COLLECTION_SIZE_RATIO` (default 0.25); read ends "will always be spilled to disk" (`tool_resource_scaling.md` [16]). I could not retrieve the MarkDuplicates section of the Picard page in this pass, so threading there is **not found**.
- **Observed** (own trace): 6 cpus requested but about 102 % CPU, i.e. effectively single-threaded; p95 RSS 31.7 GB against 38.7 GB requested, which is roughly the 80 % heap (`-Xmx`). AWS rnaseq: memory and runtime correlate with input (rho 0.59 to 0.89); CPU efficiency 0.23.
- **Important caveat** (inference): because `-Xmx` is derived from the request, **peak RSS follows the request** and cannot show the true need. The AWS correlation of memory with input may partly reflect this. The real minimum has to be found by lowering the memory request in a sweep and watching for failures or slowdown from spilling.
- **Consequence** (inference): `cpus` can probably be reduced a lot (single-threaded); memory needs a dedicated sweep.
- **Likely outcome** (inference): `cpus = 1` or 2, memory from a sweep with input size, runtime from input size.

## Template for further modules

Fill in: label and current values; module code lines that use `task.cpus` and `task.memory`; tool options that set threads or memory; what the docs say about scaling; own and AWS observations; caveats such as versions; likely experiment. Use `docs/tool_resource_scaling.md` and `docs/tool_scaling_evidence.csv` for modules covered there.

## Sources

- nf-core/rnaseq `conf/base.config`: https://github.com/nf-core/rnaseq/blob/master/conf/base.config
- nf-core modules: `modules/nf-core/fastqc/main.nf`, `modules/nf-core/trimgalore/main.nf`, `modules/nf-core/picard/markduplicates/main.nf` (master, read 2026-11-17)
- FastQC script help text: https://github.com/s-andrews/FastQC/blob/master/fastqc
- Trim Galore README: https://github.com/FelixKrueger/TrimGalore
- Picard command-line overview: https://broadinstitute.github.io/picard/command-line-overview.html
