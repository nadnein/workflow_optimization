# Nextflow Trace Resource Analysis

Notebook for exploring resource usage of **Nextflow trace files (raw format)** to optimize CPU and memory requests.

## Setup

This project uses **uv** for reproducible environments.

```bash
git clone <repo-url>
cd nf-trace-analysis
uv sync
```

## Run notebook (VS Code recommended)

```bash
uv run jupyter notebook
```

Open:
```
notebooks/trace_analysis.ipynb
```

Select the `.venv` kernel if prompted.

## Required Nextflow trace config (raw)

```groovy
trace {
  enabled = true
  raw = true
  fields = "process,task_id,name,status,cpus,memory,realtime,%cpu,peak_rss,read_bytes,write_bytes"
}
```
