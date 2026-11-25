"""Adaptive downloader for public PDFs.

Each shard runs at most MAX_CONCURRENCY downloads at a time. Errors that signal load (HTTP 429/5xx, timeouts,
connection resets) halve the shard's concurrency and add a growing pause between requests; Retry-After is honoured
for every worker of the shard. A clean streak restores speed step by step. If the error rate stays high at the
slowest setting the shard stops (circuit breaker): being blocked would hurt more than finishing later.
"""
from __future__ import annotations

import random
import threading
import time
from collections import deque

import requests

USER_AGENT = ("cse-filings-worker/1.0 (+https://github.com/dinikejayamaha-hash/cse-filings-worker; "
              "batch text extraction of public filing PDFs; adaptive rate limit)")
MAX_CONCURRENCY = 2
WINDOW = 20
MAX_PAUSE = 60.0
MAX_BYTES = 300_000_000


class CircuitOpen(RuntimeError):
    pass


class Throttle:
    def __init__(self, max_concurrency: int = MAX_CONCURRENCY, clock=time.monotonic, sleep=time.sleep):
        self.max = max_concurrency
        self.limit = max_concurrency
        self.pause = 0.0
        self.active = 0
        self.until = 0.0
        self.last = 0.0
        self.recent: deque = deque(maxlen=WINDOW)
        self.streak = 0
        self.cv = threading.Condition()
        self.clock, self.sleep = clock, sleep
        self.stats = {"requests": 0, "ok": 0, "errors": 0, "by_kind": {}, "slowdowns": 0, "min_limit": max_concurrency,
                      "max_pause": 0.0}
        self.open = False

    def acquire(self):
        with self.cv:
            while self.active >= self.limit and not self.open:
                self.cv.wait(1)
            if self.open:
                raise CircuitOpen("error rate too high: stopped")
            self.active += 1
        while True:
            with self.cv:
                now = self.clock()
                wait = max(self.until - now, self.last + self.pause - now, 0)
                if wait <= 0:
                    self.last = now
                    break
            self.sleep(min(wait, 5) + random.random() * 0.1)

    def release(self, ok: bool, kind: str | None = None, retry_after: float | None = None):
        with self.cv:
            self.active -= 1
            self.stats["requests"] += 1
            self.recent.append(ok)
            if ok:
                self.stats["ok"] += 1
                self.streak += 1
                if self.streak >= WINDOW and (self.pause or self.limit < self.max):
                    self.pause = 0.0 if self.pause < 0.5 else self.pause / 2
                    self.limit = min(self.max, self.limit + 1)
                    self.streak = 0
            else:
                self.stats["errors"] += 1
                self.stats["by_kind"][kind or "error"] = self.stats["by_kind"].get(kind or "error", 0) + 1
                self.streak = 0
                if retry_after:
                    self.until = max(self.until, self.clock() + min(retry_after, 300))
                bad = self.recent.count(False)
                if bad >= 2 and bad / len(self.recent) >= 0.2:
                    if self.limit > 1 or self.pause < MAX_PAUSE:
                        self.limit = max(1, self.limit // 2)
                        self.pause = min(MAX_PAUSE, max(1.0, self.pause * 2))
                        self.stats["slowdowns"] += 1
                        self.recent.clear()
                    elif len(self.recent) >= WINDOW // 2 and bad / len(self.recent) >= 0.5:
                        self.open = True
                self.stats["min_limit"] = min(self.stats["min_limit"], self.limit)
                self.stats["max_pause"] = max(self.stats["max_pause"], self.pause)
            self.cv.notify_all()


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/pdf,application/json;q=0.9,*/*;q=0.5"})
    return s


def get_pdf(s: requests.Session, th: Throttle, urls: list[str], attempts: int = 5) -> tuple[bytes | None, str, str]:
    """(bytes, url_used, status): status ok | gone (404/410 on every URL) | forbidden | error (transient, retry
    next run) | too_large. Each HTTP request passes through the throttle."""
    status = "error"
    for url in urls:
        for attempt in range(attempts):
            th.acquire()
            ok, kind, ra = False, None, None
            try:
                r = s.get(url, timeout=(20, 180), stream=True)
                code = r.status_code
                if code == 200:
                    size = int(r.headers.get("Content-Length") or 0)
                    if size > MAX_BYTES:
                        r.close()
                        ok = True
                        return None, url, "too_large"
                    data = r.content
                    ok = True
                    if len(data) > MAX_BYTES:
                        return None, url, "too_large"
                    return data, url, "ok"
                r.close()
                if code in (404, 410):
                    ok, status = True, "gone"
                    break
                if code in (401, 403):        # expected on a primary URL with a fallback; on the last URL it
                    status = "forbidden"      # may mean a block, so it counts as an error for the throttle
                    ok, kind = url != urls[-1], "http_403"
                    break
                kind = f"http_{code}"
                ra = _retry_after(r.headers.get("Retry-After"))
            except requests.Timeout:
                kind = "timeout"
            except requests.RequestException as e:
                kind = type(e).__name__
            finally:
                th.release(ok, kind, ra)
            time.sleep(min(60, 2 ** attempt + random.random()))
    return None, urls[-1] if urls else "", status


def _retry_after(v) -> float | None:
    try:
        return float(v) if v else None
    except ValueError:
        return 30.0
