# cse-filings-worker

Fetches public financial-report PDFs (annual reports, audited statements and interim statements) published by
listed companies on the Colombo Stock Exchange website, and extracts the text of every page.

## What it does
1. **List** (`worker/listing.py`): the exchange's public filings feed, one request per calendar year, 2010-2025.
   Order: annual reports and audited statements first, then interims; newest upload first within each group;
   uploads before June 2012 last.
2. **Extract** (`worker/extract.py`, 8 parallel shards): download each PDF, read every page's text layer with
   pypdfium2 (pdfplumber if pypdfium2 cannot open the file), and OCR pages with no text layer with tesseract
   (200 dpi, `--psm 6`). Per filing: `meta.json`, `pNNNN.txt` per page, `ocr/pNNNN.txt` per OCR'd page.
3. **Publish** (`worker/run.py publish`): the per-shard output is packed into `tar.gz` batches and **encrypted with
   [age](https://age-encryption.org)** to the public key in `AGE_RECIPIENT`, then attached to a GitHub release
   `b<run id>`. Releases are deleted after 7 days. `state/done.json` records which filing ids are finished
   (status only) so the next run continues where this one stopped; `state/runs.jsonl` logs each run's counts,
   HTTP error rate and throughput.

## What is not here
No extracted text, PDFs, prices, databases or analysis are ever committed or published in plain text. The
repository holds only generic code, the age *public* key, and the done-list of filing ids. There are no secrets:
the workflow uses only the repository's own `GITHUB_TOKEN`.

## Load on the exchange's servers
- At most 8 shards x 2 concurrent downloads (16 requests in flight), with a descriptive User-Agent.
- Each shard backs off on its own: HTTP 429/5xx, timeouts and connection errors halve its concurrency and add a
  pause that doubles up to 60 s; `Retry-After` is honoured; a clean streak restores speed gradually.
- Circuit breaker: if errors stay above 50% at the slowest setting, the shard stops for this run.
- Retries with exponential backoff; 404s are recorded and not retried.

## Security
- Triggers: `schedule` and `workflow_dispatch` only. No pull-request or comment triggers, so forks never run
  workflows with this repository's token.
- Top-level `permissions: contents: read`; only the publish job (which never opens a PDF) gets `contents: write`.
- Every action is pinned to a full commit SHA; Python dependencies install from a hash-locked
  `requirements.lock`.
- `tests/test_worker.py` fails if any file looks like a secret or a data file, or if a workflow gains another
  trigger.

## Run locally
```
pip install --require-hashes -r requirements.lock      # plus apt: tesseract-ocr age
python -m pytest -q
python -m worker plan --max-items 20
python -m worker shard --index 0 --of 1 --budget-minutes 5
```

MIT licence.
