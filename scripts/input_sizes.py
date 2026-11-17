#!/usr/bin/env python3
"""
Input data size per sample for the runs in a tasks__*.parquet written by scripts/megatest_waste.py.

For every run: read the newest params_*.json in its pipeline_info/, take the `input` samplesheet, parse it and look
up the size of every file it lists with a HEAD request (nothing is downloaded). Sizes are summed per sample, so they
can be joined to the task tag (`tasks.tag == sample`) in scripts/module_candidates.py.

Only samplesheets reachable on the web or S3 and listing absolute file URLs can be used; ftp:// and gs:// files are
counted as unsized. Runs without such a samplesheet get no rows, and nothing is guessed for them.

Usage:
  uv run python scripts/input_sizes.py                      # newest tasks__*.parquet in megatest_summary/
  uv run python scripts/input_sizes.py --data megatest_summary/tasks__all__since-2023-01-01.parquet
"""
import argparse, csv, io, json, re, sys, threading, urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from megatest_waste import BUCKET, ls, s3 as pipeline_s3, UNSIGNED, Config
import boto3

FILE_URL = re.compile(r"^(s3|https?|ftp|gs)://\S+$")
FILE_EXT = re.compile(r"\.(fastq|fq|bam|cram|fa|fasta|fna|vcf|bed|h5|h5ad|mzml|raw|fast5|pod5|tif|tiff|idat|gz)$|\.(fastq|fq)\.gz$", re.I)
SAMPLE_COLUMNS = ("sample", "sample_id", "sampleid", "sample_name", "id", "name")
HEADERS = {"User-Agent": "megatest-input-sizes"}
# short timeouts and few retries: a dead or slow host must fail fast instead of stalling a worker for minutes
anon = boto3.client("s3", config=Config(signature_version=UNSIGNED, connect_timeout=10, read_timeout=20,
                                        retries={"max_attempts": 2}))
_sizes = {}  # url -> bytes or None, shared across runs (the same test data is used by many runs)
_lock = threading.Lock()


def _http(url, method="GET", timeout=15):
    return urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS, method=method), timeout=timeout)


def file_size(url):
    """Size in bytes of a remote file via HEAD, or None if it cannot be determined."""
    if _sizes.get(url) is not None:
        return _sizes[url]
    size = None
    try:
        if url.startswith("s3://"):
            u = urlparse(url)
            size = anon.head_object(Bucket=u.netloc, Key=u.path.lstrip("/"))["ContentLength"]
        elif url.startswith("http"):
            size = int(_http(url, "HEAD").headers["Content-Length"])
    except Exception:
        size = None
    _sizes[url] = size
    return size


MAX_SHEET_BYTES = 5_000_000  # a samplesheet is tiny; anything bigger is not one (some `input` params point to huge data files)


def read_text(url):
    url = re.sub(r"^https://github\.com/([^/]+/[^/]+)/blob/", r"https://raw.githubusercontent.com/\1/", url)
    if url.startswith("s3://"):
        u = urlparse(url)
        obj = anon.get_object(Bucket=u.netloc, Key=u.path.lstrip("/"))
        if obj["ContentLength"] > MAX_SHEET_BYTES:
            obj["Body"].close()
            raise ValueError("too large to be a samplesheet")
        return obj["Body"].read().decode(errors="ignore")
    resp = _http(url)
    if int(resp.headers.get("Content-Length") or 0) > MAX_SHEET_BYTES:
        raise ValueError("too large to be a samplesheet")
    return resp.read(MAX_SHEET_BYTES).decode(errors="ignore")


def sheet_rows(text):
    """Rows of a CSV/TSV samplesheet as dicts."""
    delim = "\t" if text.split("\n", 1)[0].count("\t") > text.split("\n", 1)[0].count(",") else ","
    return list(csv.DictReader(io.StringIO(text), delimiter=delim))


def run_sample_files(pipeline, run):
    """({sample: [file URLs]}, reason) for one run, from its samplesheet; no sizes yet."""
    d = f"{pipeline}/{run}/pipeline_info/"
    params = [o for o in ls(d, delimiter=False) if o["Key"].rsplit("/", 1)[-1].startswith("params")]
    if not params:
        return {}, "no params file"
    newest = max(params, key=lambda o: o["LastModified"])
    p = json.loads(pipeline_s3.get_object(Bucket=BUCKET, Key=newest["Key"])["Body"].read().decode(errors="ignore"))
    sheet = p.get("input")
    if not sheet or not str(sheet).startswith(("http", "s3://")):
        return {}, "no web/S3 samplesheet in params"
    try:
        rows = sheet_rows(read_text(str(sheet)))
    except Exception as e:
        return {}, f"samplesheet not readable ({type(e).__name__})"
    if not rows:
        return {}, "empty samplesheet"
    cols = list(rows[0])
    sample_col = next((c for c in cols if c.lower() in SAMPLE_COLUMNS), cols[0])
    out = {}
    for r in rows:
        sample = (r.get(sample_col) or "").strip()
        files = [v.strip() for k, v in r.items() if k != sample_col and v and FILE_URL.match(v.strip()) and FILE_EXT.search(v.strip())]
        if sample and files:
            out.setdefault(sample, []).extend(files)
    return (out, "ok") if out else ({}, "samplesheet lists no absolute file URLs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, help="tasks parquet to use (default: newest tasks__*.parquet in --out)")
    ap.add_argument("--out", default="megatest_summary", type=Path)
    a = ap.parse_args()
    data = a.data or max(a.out.glob("tasks__*.parquet"), key=lambda f: f.stat().st_mtime, default=None)
    if data is None:
        sys.exit(f"No tasks__*.parquet in {a.out}; run scripts/megatest_waste.py first")
    runs = list(pd.read_parquet(data, columns=["pipeline", "run"]).drop_duplicates().itertuples(index=False))

    # progress is cached on disk, so an interrupted run (e.g. network lost) continues where it stopped.
    # Only definite results are cached: failed lookups (size None, exceptions) are retried on the next run.
    cache_file = a.out / "input_sizes_cache.json"
    cache = json.loads(cache_file.read_text()) if cache_file.exists() else {"sizes": {}, "runs": {}}
    _sizes.update({u: v for u, v in cache["sizes"].items()})

    def save():
        with _lock:
            cache["sizes"] = {u: v for u, v in _sizes.items() if v is not None}
            cache_file.write_text(json.dumps(cache))

    definite = ("ok", "no params file", "no web/S3 samplesheet in params", "empty samplesheet", "samplesheet lists no absolute file URLs")
    done = [0]

    def work(r):
        key = f"{r.pipeline}/{r.run}"
        if key in cache["runs"]:
            return r, *cache["runs"][key]
        try:
            res = run_sample_files(r.pipeline, r.run)
        except Exception as e:  # one broken run must not abort the rest; not cached, retried next time
            return r, {}, f"error ({type(e).__name__})"
        if res[1] in definite:
            cache["runs"][key] = list(res)
        done[0] += 1
        if done[0] % 25 == 0:
            print(f"  samplesheets read for {done[0]} runs (+ {len(cache['runs'])} cached)", file=sys.stderr, flush=True)
            save()
        return r, *res

    # phase 1: samplesheets of all runs; phase 2: every distinct file URL sized once, in one large parallel batch
    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(work, runs))
    save()
    urls = sorted({f for _, m, _ in results for fs in m.values() for f in fs if _sizes.get(f) is None})
    print(f"{len(runs)} runs, {len(urls)} input files still to size", file=sys.stderr, flush=True)
    with ThreadPoolExecutor(32) as ex:
        for n, _ in enumerate(ex.map(file_size, urls), 1):
            if n % 100 == 0:
                print(f"  sized {n}/{len(urls)}", file=sys.stderr, flush=True)
                save()
    save()

    rows = []
    for r, mapping, _ in results:
        for sample, files in mapping.items():
            sizes = [_sizes[f] for f in files]
            rows.append(dict(pipeline=r.pipeline, run=r.run, sample=sample,
                             input_bytes=sum(x for x in sizes if x is not None),
                             n_files=len(files), n_sized=sum(x is not None for x in sizes)))
    print("runs by outcome:\n" + pd.Series([why for _, _, why in results]).value_counts().to_string())
    if not rows:
        sys.exit("No input sizes could be determined.")
    df = pd.DataFrame(rows)
    if df["n_sized"].sum() == 0:  # e.g. network lost: do not write a file that would look like real data
        sys.exit("No input file could be sized (network problem?). Nothing written; rerun to continue from the cache.")
    path = a.out / f"input_sizes__{data.stem.removeprefix('tasks__')}.parquet"
    df.to_parquet(path)
    print(f"\n{df[['pipeline', 'run']].drop_duplicates().shape[0]} runs, {len(df)} samples, "
          f"{int(df.n_sized.sum())} of {int(df.n_files.sum())} files sized; written to {path}")


if __name__ == "__main__":
    main()
