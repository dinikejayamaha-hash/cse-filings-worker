"""Command line for the three workflow jobs.

    python -m worker plan    --out worklist.json --max-items 3000
    python -m worker shard   --worklist worklist.json --index 0 --of 8 --budget-minutes 140 --dist dist
    python -m worker publish --dist dist --run-id 123

`shard` writes only encrypted batches (`*.tar.gz.age`, age recipient in AGE_RECIPIENT) and a manifest of
statuses (no text) to --dist; the plaintext working copy stays on the runner and is deleted.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import extract, fetch, listing

ROOT = Path(__file__).resolve().parents[1]
DONE = ROOT / "state/done.json"
RUNS = ROOT / "state/runs.jsonl"
RECIPIENT = ROOT / "AGE_RECIPIENT"
FINAL = {"ok", "bad_pdf", "gone", "too_large"}      # never retried; "error"/"forbidden"/"skipped" are retried
CHUNK_FILES = 250
CHUNK_BYTES = 400_000_000
KEEP_DAYS = 7


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def recipient() -> str:
    r = RECIPIENT.read_text().strip()
    if not r.startswith("age1") or len(r) < 50:
        raise SystemExit("AGE_RECIPIENT is not an age X25519 public key")
    return r


def plan(a) -> int:
    res = listing.main_plan(fetch.session(), str(DONE), a.out, a.max_items)
    print(json.dumps(res))
    return 0


def _pack(work: Path, fids: list[int], dist: Path, stem: str, rcpt: str) -> list[str]:
    """tar.gz chunks of finished filings, each encrypted with age; plaintext removed as soon as it is packed."""
    names, chunk, size, k = [], [], 0, 0

    def flush():
        nonlocal chunk, size, k
        if not chunk:
            return
        plain = work / f"{stem}-c{k}.tar.gz"
        with tarfile.open(plain, "w:gz") as t:
            for fid in chunk:
                t.add(str(work / str(fid)), arcname=str(fid))
        enc = dist / f"{stem}-c{k}.tar.gz.age"
        subprocess.run(["age", "-r", rcpt, "-o", str(enc), str(plain)], check=True)
        plain.unlink()
        for fid in chunk:
            shutil.rmtree(work / str(fid), ignore_errors=True)
        names.append(enc.name)
        chunk, size, k = [], 0, k + 1

    for fid in fids:
        d = work / str(fid)
        if not d.exists():
            continue
        chunk.append(fid)
        size += sum(p.stat().st_size for p in d.rglob("*") if p.is_file())
        if len(chunk) >= CHUNK_FILES or size >= CHUNK_BYTES:
            flush()
    flush()
    return names


def shard(a) -> int:
    t0 = time.time()
    deadline = t0 + a.budget_minutes * 60
    rcpt = recipient()
    items = json.load(open(a.worklist))[a.index::a.of]
    dist, work = Path(a.dist), Path(a.work)
    dist.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True, exist_ok=True)
    th = fetch.Throttle(a.concurrency)
    s = fetch.session()
    manifest: dict[str, dict] = {}
    lock = threading.Lock()
    it = iter(items)
    cpus = os.cpu_count() or 2
    pool = ProcessPoolExecutor(cpus)
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            with lock:
                item = next(it, None)
            if item is None or time.time() > deadline:
                return
            fid = item["filing_id"]
            try:
                pdf, used, status = fetch.get_pdf(s, th, [item["pdf_url"], *item.get("alt_urls", [])])
            except fetch.CircuitOpen:
                stop.set()
                return
            if pdf is None:
                with lock:
                    manifest[str(fid)] = {"status": status, "url": used}
                continue
            try:
                left = max(60, deadline - time.time() + 600)
                meta = pool.submit(extract.process, {**item, "url_used": used}, pdf, str(work),
                                   min(900, left)).result()
                rec = {k: meta.get(k) for k in ("status", "pages_total", "blank_pages", "seconds", "bytes")}
                rec["ocr_pages"] = len(meta.get("ocr_pages") or [])
                rec["errors"] = (meta.get("errors") or [])[:3]
            except Exception as e:  # noqa: BLE001
                rec = {"status": "error", "errors": [f"{type(e).__name__}: {e}"[:200]]}
                shutil.rmtree(work / str(fid), ignore_errors=True)
            with lock:
                manifest[str(fid)] = rec

    threads = [threading.Thread(target=worker) for _ in range(cpus + a.concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    pool.shutdown()
    order = [it_["filing_id"] for it_ in items if manifest.get(str(it_["filing_id"]), {}).get("status") == "ok"]
    stem = f"b{a.run_id}-s{a.index}"
    assets = _pack(work, order, dist, stem, rcpt)
    shutil.rmtree(work, ignore_errors=True)
    secs = time.time() - t0
    summary = {"shard": a.index, "assigned": len(items), "attempted": len(manifest),
               "ok": sum(1 for r in manifest.values() if r["status"] == "ok"),
               "ocr_filings": sum(1 for r in manifest.values() if r.get("ocr_pages")),
               "seconds": round(secs), "circuit_open": th.open, "http": th.stats, "assets": assets}
    json.dump({"summary": summary, "filings": manifest}, open(dist / f"manifest-{stem}.json", "w"))
    print(json.dumps(summary))
    return 0


def _gh(*args, check=True) -> str:
    return subprocess.run(["gh", *args], check=check, capture_output=True, text=True).stdout


def publish(a) -> int:
    dist = Path(a.dist)
    mans = sorted(dist.rglob("manifest-*.json"))
    assets = sorted(dist.rglob("*.tar.gz.age"))
    done = json.loads(DONE.read_text()) if DONE.exists() else {}
    tot = {"attempted": 0, "ok": 0, "final": 0, "retry": 0, "ocr_filings": 0, "requests": 0, "http_errors": 0,
           "by_kind": {}, "circuit_open": 0, "slowdowns": 0, "shard_seconds": 0}
    for m in mans:
        j = json.load(open(m))
        s = j["summary"]
        tot["attempted"] += s["attempted"]
        tot["ok"] += s["ok"]
        tot["ocr_filings"] += s["ocr_filings"]
        tot["requests"] += s["http"]["requests"]
        tot["http_errors"] += s["http"]["errors"]
        tot["slowdowns"] += s["http"]["slowdowns"]
        tot["circuit_open"] += int(s["circuit_open"])
        tot["shard_seconds"] = max(tot["shard_seconds"], s["seconds"])
        for k, v in s["http"]["by_kind"].items():
            tot["by_kind"][k] = tot["by_kind"].get(k, 0) + v
        for fid, r in j["filings"].items():
            if r["status"] in FINAL:
                done[fid] = r["status"]
                tot["final"] += 1
            else:
                tot["retry"] += 1
    tot["error_rate"] = round(tot["http_errors"] / tot["requests"], 4) if tot["requests"] else None
    tot["filings_per_hour"] = round(tot["attempted"] / tot["shard_seconds"] * 3600) if tot["shard_seconds"] else None
    tag = f"b{a.run_id}"
    if assets:
        _gh("release", "create", tag, *map(str, assets), "--title", f"Batch {a.run_id}",
            "--notes", "Encrypted page-text batches (age). Removed after 7 days.", "--latest=false")
    pruned = prune() if not a.no_prune else 0
    DONE.parent.mkdir(exist_ok=True)
    DONE.write_text(json.dumps(dict(sorted(done.items(), key=lambda kv: int(kv[0]))), separators=(",", ":")))
    rec = {"at": _now(), "run_id": a.run_id, "release": tag if assets else None, "assets": len(assets),
           "done_total": len(done), "pruned_releases": pruned, **tot}
    with open(RUNS, "a") as fh:
        fh.write(json.dumps(rec) + "\n")
    print(json.dumps(rec))
    return 0


def prune(days: int = KEEP_DAYS) -> int:
    rels = json.loads(_gh("release", "list", "--limit", "500", "--json", "tagName,createdAt") or "[]")
    cut = datetime.now(timezone.utc) - timedelta(days=days)
    n = 0
    for r in rels:
        if r["tagName"].startswith("b") and datetime.fromisoformat(r["createdAt"].replace("Z", "+00:00")) < cut:
            _gh("release", "delete", r["tagName"], "--yes", "--cleanup-tag", check=False)
            n += 1
    return n


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="worker")
    sub = p.add_subparsers(dest="cmd", required=True)
    a1 = sub.add_parser("plan")
    a1.add_argument("--out", default="worklist.json")
    a1.add_argument("--max-items", type=int, default=3000)
    a2 = sub.add_parser("shard")
    a2.add_argument("--worklist", default="worklist.json")
    a2.add_argument("--index", type=int, required=True)
    a2.add_argument("--of", type=int, default=8)
    a2.add_argument("--budget-minutes", type=float, default=140)
    a2.add_argument("--concurrency", type=int, default=fetch.MAX_CONCURRENCY)
    a2.add_argument("--dist", default="dist")
    a2.add_argument("--work", default="work")
    a2.add_argument("--run-id", default="local")
    a3 = sub.add_parser("publish")
    a3.add_argument("--dist", default="dist")
    a3.add_argument("--run-id", required=True)
    a3.add_argument("--no-prune", action="store_true")
    a = p.parse_args(argv)
    if a.cmd == "shard" and not 1 <= a.concurrency <= fetch.MAX_CONCURRENCY:
        p.error(f"--concurrency must be 1..{fetch.MAX_CONCURRENCY}")
    return {"plan": plan, "shard": shard, "publish": publish}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
