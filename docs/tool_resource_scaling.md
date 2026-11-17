# Which tools should scale with input size? Literature notes

Purpose: the AWS megatest data does not contain enough runs per module to show whether CPU or memory use
follows input size. This file collects what the **tools' own documentation and papers** say, so candidates
can be chosen on prior knowledge and then checked against the data (`scripts/module_candidates.py`).

Compiled 2026-11-12 by an AI assistant (Claude) from pages it retrieved in that session. Every statement carries an
evidence level:

- **Documented**: the retrieved page says it (quoted or closely paraphrased).
- **Not stated**: the retrieved page was read and says nothing about it. This is not evidence of "no dependence".
- **Inference**: reasoning by the assistant, not from a source. Treat as a hypothesis to test.
- **Excerpt only**: seen only in a search-result excerpt, the page itself was not opened.
- **Not retrieved**: the fetch failed (404 or truncated); nothing is claimed.

Nothing here was checked against the AWS data except where stated. Please verify the sources before citing them.
The pages are the current documentation; older tool versions in the AWS runs may behave differently.

## Tools checked

| Tool (module) | Memory driven by | Runtime driven by | Evidence |
|---|---|---|---|
| STAR (`STAR_ALIGN`) | Genome size: "at least 10 x GenomeSize bytes", about 30 GB for human, 32 GB recommended; plus per-thread buffers. | Not stated. | Documented [1]. Reads not driving memory is an inference. |
| Kraken 2 (`KRAKEN2`) | Database size: RAM must "hold the database (primarily the hash table)"; default database 29 GB. | Not stated; no classification runtime figures. | Documented [2]. |
| BWA-MEM2 (`BWAMEM2_ALIGN`) | Reference size. Index step "Requires 28N GB memory where N is the size of the reference sequence". | Not stated in text ("~1.3-3.1x faster" than BWA-MEM). | Documented [3]. |
| Flye (`FLYE`) | Genome size and coverage; "several hundreds of RAM" for large genomes at high coverage; `--asm-coverage` limits it. Benchmark: 4.6 Mb at 50x about 2 GB RAM, human HiFi 30x about 141 GB. | Same table: 2 h CPU time up to 780 h. | Documented [4]. No formula given. |
| fastp (`FASTP`) | Mostly independent of input: the optional duplication estimate uses 1 GB to 24 GB depending on the accuracy level. | Not stated how it scales with reads. | Documented [5]. |
| BUSCO (`BUSCO`) | Not stated. | Gene predictor (Miniprot, Metaeuk, Augustus), `--long`, lineage dataset, CPUs (default 1). | Documented [6]. Dependence on genome size: not stated. |
| Filtlong (`FILTLONG`) | Not stated. | Not stated. | Not stated [7]. |
| FastQC (`FASTQC`) | Not stated. | Not stated. | Not stated [8]. |
| Trim Galore (`TRIMGALORE`) | A configurable budget: "Memory budget is controlled by the global `--memory` flag (default `1G`)" (matters for `--clumpify`). | Cores: "near-linear speedup up to `--cores 8` for paired-end runs", gzip output becomes the bottleneck beyond that. | Documented [9]. Input-size scaling: not stated. The fetched README appears to describe a recent major version; older 0.6.x behaviour is not covered. |
| Unicycler (`UNICYCLER`) | Not stated. | "Two main factors influence the running time: the number of long reads ... and the genome size/complexity"; subsampling >100x long reads makes it faster; typical "an hour or so" to "12 hours ... or more". | Documented [10]. Memory: not stated. |
| MEGAHIT (`MEGAHIT`) | Not stated in the README, apart from a hint that memory can be a problem at low depth when building the k_min graph (`--kmin-1pass`). | Not stated. | Documented [11] (hint only). |
| SPAdes (`SPADES`) | `-m` limit, default 250 GB; "Actual amount of RAM consumed will be below this limit". `--frugal` trades contiguity for memory. | Not stated. | Documented [12]. No requirements or dependencies given. |
| samtools sort (`SAMTOOLS_SORT`) | `-m` is "approximately the maximum required memory per thread", default 768 MiB; data beyond memory is spilled to temporary files. | Not stated. | Documented [13]. Memory therefore bounded by the setting, not the input: inference from the spill behaviour. |
| minimap2 (`MINIMAP2_ALIGN`) | Not stated. Query is processed in batches (`-K` 200M bases), reference in `-I` (4G bases) chunks. | Index building "takes a few minutes" per reference. | Documented [14]. Read-count dependence: not stated. |
| Salmon (`SALMON_QUANT`) | Index; `--gcBias` needs "an extra 4-bytes per nucleotide" (`--reduceGCMemory` lowers it). | Threads; in quasi-mapping, finding mapping locations is the slowest step; bootstraps add time. | Documented [15]. Scaling with number of reads: not stated. |
| Picard MarkDuplicates (`PICARD_MARKDUPLICATES`) | Java heap plus `SORTING_COLLECTION_SIZE_RATIO` (default 0.25): "this number, plus the maximum RAM available to the JVM, determine the memory footprint"; read ends "will always be spilled to disk". | Not stated. | Documented [16]. Input-size dependence: not stated. |
| bcftools mpileup (`BCFTOOLS_MPILEUP`) | "can require more memory when multiple regions and many alignment files are processed". | Not stated; `--full-BAQ` is slower than the default heuristics. | Documented [17]. Only the first part of the long manual was read. |
| Prokka (`PROKKA`) | Not stated. | Not stated; `--cpus` default 8. | Not stated [18]. |
| preseq (`PRESEQ_LCEXTRAP`) | Not stated. | Not stated (the manual in `docs/` was not read). | Not stated [19]. |
| Porechip_ABI (`PORECHOP_ABI`, `PORECHOP_PORECHOP`) | Not retrieved: the GitHub README returned 404. | Not retrieved. A search found the paper [20]; the excerpt does not cover memory or runtime. | Not retrieved. |

## What this suggests for the input-size question

All of the following are **inferences**, to be tested on data:

- Reference-bound tools (STAR, BWA-MEM2, Kraken 2, Salmon index) have memory set mostly by the genome, database or index,
  so memory should not rise with the number of reads; their runtime is the more likely thing to follow read volume.
  Correlating peak memory with input size for these would mostly show noise.
- Tools whose memory is governed by a setting (samtools sort `-m`, Picard sorting ratio and heap, Trim Galore `--memory`,
  SPAdes `-m`) should show peak memory that follows the configured value rather than the input. Runtime and temporary
  disk use are the better targets there.
- Assemblers have the strongest documented link: Flye (genome size and coverage, benchmark table) and Unicycler
  (runtime from long-read count and genome complexity). MEGAHIT and SPAdes are plausible but their docs give no figures.
- For tools whose documentation is silent (Filtlong, FastQC, Prokka, preseq, minimap2 memory), only data can answer.
- Observation from our own data (not a source): `STAR_ALIGN` shows a 95th-percentile peak memory of about 39 GB over
  296 tasks, the same range as the documented 30 GB for a human genome, against requests of 77 GB. `TRIMGALORE` shows a
  95th-percentile peak of about 6.7 GB against a 77 GB median request.

## Further modules worth checking (outside the first candidate list)

Chosen from the AWS data (several pipelines, many tasks, long total runtime) and not yet researched unless listed above.
The reason given is the data criterion; the scaling hypothesis is open.

| Module | Pipelines | Tasks | Total runtime (h) | Note |
|---|---|---|---|---|
| `MEGAHIT` | 3 | 379 | 626 | Assembler; docs reviewed above, no figures. |
| `GATK4_SPLITNCIGARREADS` | 2 | 1,273 | 189 | Not researched. |
| `QUAST` | 7 | 970 | 96 | Not researched. |
| `PRODIGAL` | 2 | 647 | 99 | Not researched. |
| `BEDTOOLS_GENOMECOV` | 6 | 319 | 64 | Not researched. |
| `BOWTIE2_ALIGN` | 7 | 416 | 31 | The manual URL tried returned 404; not retrieved. |
| `GATK4_MARKDUPLICATES` | 5 | 77 | 28 | Not researched. |
| `STAR_GENOMEGENERATE` | 6 | 66 | 33 | Index build; the STAR protocol [1] gives the RAM rule. |
| `SAMTOOLS_MERGE`, `SALMON_INDEX`, `CNVKIT_BATCH`, `CHOPPER`, `NANOQ` | 2-4 | 31-97 | 10-18 | Not researched. |

## General background (workflow resource prediction)

- Bader et al. (2024), *Sizey: Memory-Efficient Execution of Scientific Workflow Tasks* [21]. The abstract (retrieved) says
  that "the resource consumption for the same task type can vary considerably as well due to different inputs", and the
  method is evaluated on nf-core workflows. The abstract does **not** state that memory correlates with input size, so
  that should not be quoted from it.
- Bader et al., *Lotaru* [22] (excerpt only): per the search excerpt, it predicts task runtimes with a linear model of input
  size and notes this suits tasks that process several input files in a data-parallel way. Not opened.

## How to find this information for other modules

1. The tool's own documentation or README: look for "memory", "RAM", "requirements", "benchmark" (as done above).
2. The tool's paper, especially supplementary benchmark tables with CPU time and RAM against dataset size.
3. nf-core module pages (`meta.yml`, `main.nf`) for the process label, which is the default request and not a measurement.
4. The megatest data itself: `scripts/module_detail.py MODULE` lists every run, and `scripts/module_candidates.py` gives the
   input-size correlation columns where an input size or `rchar` is available.

## Sources (retrieved 2026-11-12)

1. Dobin A, Gingeras TR. *Mapping RNA-seq Reads with STAR.* https://pmc.ncbi.nlm.nih.gov/articles/PMC4631051 (Basic Protocol, Necessary Resources; Troubleshooting)
2. Kraken 2 manual. https://github.com/DerrickWood/kraken2/wiki/Manual (System Requirements)
3. BWA-MEM2 README. https://github.com/bwa-mem2/bwa-mem2 (Important Information, Usage)
4. Flye usage documentation. https://github.com/fenderglass/Flye/blob/flye/docs/USAGE.md (Reducing RAM consumption; Flye benchmarks)
5. fastp README. https://github.com/OpenGene/fastp (duplication rate evaluation; all options)
6. BUSCO user guide. https://busco.ezlab.org/busco_userguide.html
7. Filtlong README. https://github.com/rrwick/Filtlong
8. FastQC install notes. https://www.bioinformatics.babraham.ac.uk/projects/fastqc/INSTALL.txt
9. Trim Galore README. https://github.com/FelixKrueger/TrimGalore (Features; Usage; Output files)
10. Unicycler README. https://github.com/rrwick/Unicycler (Tips > Running time)
11. MEGAHIT README. https://github.com/voutcn/megahit (Advanced usage)
12. SPAdes manual. https://ablab.github.io/spades/running.html (Advanced options; Pipeline options)
13. samtools sort manual. https://www.htslib.org/doc/samtools-sort.html
14. minimap2 README. https://github.com/lh3/minimap2 (General usage; Algorithm overview)
15. Salmon documentation. https://salmon.readthedocs.io/en/latest/salmon.html
16. Picard command-line overview. https://broadinstitute.github.io/picard/command-line-overview.html (MarkDuplicates section)
17. bcftools manual. https://samtools.github.io/bcftools/bcftools.html (mpileup)
18. Prokka README. https://github.com/tseemann/prokka
19. preseq README. https://github.com/smithlabcode/preseq
20. Porechop_ABI paper (search excerpt only). https://pmc.ncbi.nlm.nih.gov/articles/PMC9869717
21. Bader J et al. *Sizey.* https://arxiv.org/abs/2407.16353
22. Bader J et al. *Lotaru.* https://arxiv.org/abs/2205.11181 and https://arxiv.org/pdf/2309.06918 (excerpt only)
