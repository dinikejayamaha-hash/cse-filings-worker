"""List public CSE financial-report filings and order them into a worklist.

Source: the exchange website's public filings feed (`getFinancialAnnouncement`, one POST per calendar year), the
same feed the website's "Financial Reports" page uses. Each row has an id, a ticker, a title, an upload time and the
PDF's path on cdn.cse.lk.

Order: annual reports and audited statements first (tier 1), then interim statements (tier 2), newest upload first
within a tier, for uploads in the main window; uploads before the window follow last, in the same order.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime

API = "https://www.cse.lk/api/"
CDN = "https://cdn.cse.lk/"
MAIN_FROM = "2012-06-01"
LIST_FROM = 2010
LIST_TO = 2025
TIERS = {"annual_report": 1, "audited_fs": 1, "interim": 2}


def classify(title: str | None) -> str:
    t = (title or "").lower()
    if "errata" in t:
        return "errata"
    if "annual report" in t:
        return "annual_report"
    if "interim financial" in t or "quarter" in t:
        return "interim"
    if "audited financ" in t:
        return "audited_fs"
    if "prospectus" in t or "introductory document" in t:
        return "prospectus"
    return "other"


def parse_uploaded(s: str | None) -> str | None:
    for fmt in ("%d %b %Y %I:%M:%S %p", "%d %b %Y %H:%M:%S", "%d %b %Y"):
        try:
            return datetime.strptime((s or "").strip(), fmt).isoformat()
        except ValueError:
            continue
    return None


def urls(path: str) -> tuple[str, list[str]]:
    """Primary URL and fallbacks: older uploads are served only under the cdn's cmt/ prefix."""
    p = (path or "").lstrip("/")
    alt = [] if p.startswith("cmt/") else [CDN + "cmt/" + p]
    return CDN + p, alt


def items_from_rows(rows: list[dict], end: str | None = None) -> list[dict]:
    out, seen = [], set()
    for r in rows:
        fid, kind, path = r.get("id"), classify(r.get("fileText")), (r.get("path") or "").strip()
        up = parse_uploaded(r.get("uploadedDate"))
        if not isinstance(fid, int) or fid in seen or kind not in TIERS or not path or not r.get("symbol") or not up:
            continue
        if end and up[:10] > end:
            continue
        seen.add(fid)
        url, alt = urls(path)
        out.append({"filing_id": fid, "symbol": r.get("symbol"), "kind": kind, "tier": TIERS[kind],
                    "upload_date": up, "pdf_url": url, "alt_urls": alt})
    return order(out)


def order(items: list[dict], main_from: str = MAIN_FROM) -> list[dict]:
    def key(it):
        inv = "".join(chr(0x10FFFF - ord(ch)) for ch in it["upload_date"])     # newest first
        return (it["upload_date"][:10] < main_from, it["tier"], inv, -it["filing_id"])
    return sorted(items, key=key)


def fetch_rows(session, years=range(LIST_FROM, LIST_TO + 1), pause: float = 1.0) -> list[dict]:
    """One POST per calendar year; retries with backoff. Raises if a year cannot be listed (a partial list would
    silently reorder the work)."""
    rows = []
    for y in years:
        for attempt in range(5):
            try:
                r = session.post(API + "getFinancialAnnouncement",
                                 data={"fromDate": f"{y}/01/01", "toDate": f"{y}/12/31"}, timeout=120)
                if r.status_code == 200:
                    j = r.json()
                    got = next((v for v in j.values() if isinstance(v, list)), []) if isinstance(j, dict) else j
                    rows += got
                    print(f"listing {y}: {len(got)} rows", flush=True)
                    break
                wait = float(r.headers.get("Retry-After") or 0) or 2 ** (attempt + 1)
                print(f"listing {y}: HTTP {r.status_code}, retry in {wait:.0f}s", flush=True)
            except (ValueError, OSError) as e:
                wait = 2 ** (attempt + 1)
                print(f"listing {y}: {type(e).__name__}, retry in {wait}s", flush=True)
            time.sleep(min(wait, 120))
        else:
            raise RuntimeError(f"could not list filings for {y}")
        time.sleep(pause)
    return rows


def worklist(rows: list[dict], done: dict, max_items: int, end: str | None = f"{LIST_TO}-12-31") -> list[dict]:
    return [it for it in items_from_rows(rows, end) if str(it["filing_id"]) not in done][:max_items]


def main_plan(session, done_path: str, out_path: str, max_items: int) -> dict:
    try:
        done = json.load(open(done_path))
    except (OSError, ValueError):
        done = {}
    rows = fetch_rows(session)
    wl = worklist(rows, done, max_items)
    allitems = items_from_rows(rows, f"{LIST_TO}-12-31")
    json.dump(wl, open(out_path, "w"))
    left = sum(1 for it in allitems if str(it["filing_id"]) not in done)
    return {"listed": len(allitems), "done": len(done), "remaining": left, "this_run": len(wl),
            "tier1_remaining": sum(1 for it in allitems if it["tier"] == 1 and str(it["filing_id"]) not in done),
            "as_of": date.today().isoformat()}
