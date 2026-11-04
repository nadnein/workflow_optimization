# Nextflow Pipeline Resource Analysis

Interactive notebook for analyzing and comparing Nextflow pipeline runs. Load trace files to identify resource optimization opportunities with three complementary dashboards:

- **Optimization Priority Ranking** — Score processes by impact, inefficiency, and variability to find the best tuning targets
- **Resource Burden Views** — Compare CPU, memory, runtime, and I/O contribution across processes
- **Scaling Explorer** — Track how consistent processes scale across the runs of a pipeline

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

Trace files are discovered automatically: put the `*.txt` traces in a folder under `data/` whose name contains the pipeline name (e.g. `data/rnaseq/`) and set `PIPELINE` in the notebook.

Open:
```
notebooks/Resource Contribution Scaling Analysis.ipynb
```

Select the `.venv` kernel if prompted.

## Required Nextflow trace config

Add this to your `nextflow.config` to generate compatible trace files:

```groovy
trace {
  enabled = true
  raw = true
  fields = "process,task_id,name,status,cpus,memory,realtime,time,%cpu,peak_rss,read_bytes,write_bytes"
}
```

**Key fields:**
- `cpus`, `memory`, `time` — requested resources  
- `%cpu`, `peak_rss`, `realtime` — actual usage
- `read_bytes`, `write_bytes` — I/O volume (optional but recommended)
