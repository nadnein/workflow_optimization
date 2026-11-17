# How other approaches derive resource predictions

Retrieved 2026-11-17. Evidence levels: **read** = I read the method text of the paper (arXiv HTML); **summary** = only a search-result or survey summary, not the paper itself; **not found** = looked for and did not find it. Nothing here was checked against the papers' code.

Correction to an earlier chat answer: arXiv 2408.00047 is the **Ponder** paper (memory sizing from input size), not the time-aware segment method. The segment method is k-Segments (arXiv 2311.08185) and its follow-up KS+ (arXiv 2408.12290).

## Summary table

| Method | Predicts | Feature | Model / formula | Safety margin and failure rule | Workflows / tasks used | Evidence |
|---|---|---|---|---|---|---|
| Witt et al. (HPCS 2019, "Witt-LR") | peak memory | summed input file size | linear regression; fitted to minimise wastage rather than error | standard-deviation offset; memory doubled after a failure | IceCube IceProd traces, synthetic Pegasus traces | summary (survey) |
| Tovar et al. (2017, 2022) | memory | chunk size (2022) | two-step allocation: predicted value, then the node maximum on failure; 2022 variant starts with the node maximum for 5 tasks, then observed maximum plus margin | split tasks on failure (domain-specific) | TopEFT on coffea (2022) | summary (survey) |
| Lotaru | **runtime** | uncompressed input size | Bayesian linear regression per task; node speed correction `t_target = t_local * f`, with `f = 0.5*cpu_ratio + 0.5*io_ratio` from microbenchmarks | if Pearson r < 0.75, predict the task's median runtime instead; gives uncertainty | nf-core Eager, Chipseq, Methylseq, Atacseq, Bacass; task names not listed in the text I read | read |
| k-Segments | memory over time | total input size | runtime from a linear regression; split into k equal segments; one linear regression per segment | adds the largest positive historical error to the intercepts | nf-core workflows; which ones not confirmed | summary (search) |
| KS+ | memory over time | task inputs | dynamic segmentation, memory predicted per segment | not confirmed | two nf-core workflows | summary (search) |
| Sizey | peak memory | input size and task history | ensemble of linear regression, KNN, MLP, random forest per task and machine; best model by RAQ score | offset chosen from four candidates by least past wastage; after a failure the largest memory ever observed, then doubling | nf-core eager, methylseq, chipseq, rnaseq, mag and an image workflow (iwd); named tasks: FastQC and MarkDuplicates (rnaseq), Prokka (mag) | read |
| Ponder | peak memory | input size | one linear model per task `y = a*x + b`, asymmetric loss (underprediction costs 50 times more, lambda = 1/50) | offset 2 times the weighted standard deviation, at least 128 MB; clamp to 128 MB to 64 GB; retry with the user's default | RNA-Seq (human, 53 task types), RNA-Seq (Drosophila), Sarek, MAG, Rangeland; task names not listed in the text I read | read |

## Details worth reusing

**Sizey** (read): accuracy score `AS = mean(1 - min(|yhat - y| / y, 1))`, efficiency score `ES = 1 - yhat_i / max_j(yhat_j)`, `RAQ = (1 - alpha) * AS + alpha * ES`; the experiments used alpha = 0 and a softmax interpolation of the models. Models are fitted per task-machine configuration. No minimum sample count is given; the paper says linear models suit few data points. Tasks with few executions were filtered out. Only memory is predicted.

**Ponder** (read): with fewer than 5 finished instances it uses the maximum seen peak plus 128 MB, or the user default when the new input is larger than any seen. With 5 or more, it uses the regression only if the Pearson correlation is at least 0.3, otherwise the maximum seen plus 128 MB. Baselines: Witt-LR and the workflow's own defaults. Traces are said to be public at `CommonWorkflowScheduler/MemorySizingExperiments`.

**Lotaru** (read): trained on two input files per workflow, each downsampled to about 10 % of the original; the paper recommends at least three partitions with a combined size of at least 10 % of the downsampled file. Peak memory is not predicted, the paper says because it is only known after the task has run. The CPU benchmark always uses one thread.

## What this means for our plan

1. **Input size as the one feature, one linear model per task, an upper-bound margin and a retry rule** is the common core. Our formula step should look the same, with explicit checks (see `experiment_plan.md`).
2. **Correlation gates are used in practice**: Lotaru 0.75, Ponder 0.3. Below the gate, a flat value (median, or maximum seen plus a margin) is used. Our rule "flat plus margin where there is no relation" matches this.
3. **No method predicts `cpus`** (the survey says it does not cover thread-count prediction; Sizey and Ponder predict memory only; Lotaru predicts runtime). The `cpus` decision in our plan rests on HPC scaling guidance instead (`experiment_plan.md`, phase 4), not on workflow-prediction papers.
4. **FastQC and MarkDuplicates appear in Sizey's evaluation** (rnaseq). Their results for these tasks are in the paper's figures, which I did not read. Worth reading before our own fit, as a cross-check.
5. **Evaluation metric**: memory wastage and failure counts under a simulated replay of past tasks (Sizey, Ponder). We should evaluate the same way, with leave-one-out by sample.
6. **Most methods are online and evaluated mostly in simulation** (survey). Ours is a static formula in the config; the price is that it cannot adapt, the benefit is that it is simple to audit.

## Sources

- Sizey: https://arxiv.org/html/2407.16353v2
- Ponder: https://arxiv.org/html/2408.00047v2
- Lotaru: https://arxiv.org/html/2309.06918
- k-Segments: https://arxiv.org/pdf/2311.08185 (search summary only)
- KS+: https://arxiv.org/abs/2408.12290 (search summary only)
- Survey (Bader et al. 2025): https://arxiv.org/html/2504.20867
