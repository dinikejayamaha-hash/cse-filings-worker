import io
import json
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from worker import extract, fetch, listing, run

ROOT = Path(__file__).resolve().parents[1]


def make_pdf(pages: list[str]) -> bytes:
    """Minimal PDF, one text line per page ('' = a page without a text layer)."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>", None, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    kids = []
    for text in pages:
        body = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode() if text else b""
        objs.append(f"<< /Length {len(body)} >>\nstream\n".encode() + body + b"\nendstream")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {len(objs)} 0 R "
                    f"/Resources << /Font << /F1 3 0 R >> >> >>")
        kids.append(len(objs))
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(f'{k} 0 R' for k in kids)}] /Count {len(kids)} >>"
    out, offs = io.BytesIO(), []
    out.write(b"%PDF-1.4\n")
    for i, o in enumerate(objs, 1):
        offs.append(out.tell())
        out.write(f"{i} 0 obj\n".encode() + (o if isinstance(o, bytes) else o.encode()) + b"\nendobj\n")
    x = out.tell()
    out.write(f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode())
    for o in offs:
        out.write(f"{o:010d} 00000 n \n".encode())
    out.write(f"trailer << /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{x}\n%%EOF\n".encode())
    return out.getvalue()


ROWS = [
    {"id": 5, "symbol": "AAA", "fileText": "Interim Financial Statements 30.06.2019", "uploadedDate": "14 Aug 2019 10:00:00 AM", "path": "upload_report_file/a.pdf"},
    {"id": 6, "symbol": "BBB", "fileText": "Annual Report 2018/19", "uploadedDate": "30 Jun 2019 04:00:00 PM", "path": "/upload_report_file/b.pdf"},
    {"id": 7, "symbol": "CCC", "fileText": "Annual Report 2010/11", "uploadedDate": "30 Jun 2011 04:00:00 PM", "path": "c.pdf"},
    {"id": 8, "symbol": "DDD", "fileText": "Audited Financial Statements 2015", "uploadedDate": "01 Jul 2015 09:00:00 AM", "path": "d.pdf"},
    {"id": 9, "symbol": "EEE", "fileText": "Errata - Annual Report", "uploadedDate": "01 Jul 2016 09:00:00 AM", "path": "e.pdf"},
    {"id": 10, "symbol": None, "fileText": "Annual Report", "uploadedDate": "01 Jul 2016 09:00:00 AM", "path": "f.pdf"},
    {"id": 11, "symbol": "GGG", "fileText": "Annual Report 2026", "uploadedDate": "01 Jul 2026 09:00:00 AM", "path": "g.pdf"},
    {"id": 5, "symbol": "AAA", "fileText": "Interim Financial Statements 30.06.2019", "uploadedDate": "14 Aug 2019 10:00:00 AM", "path": "upload_report_file/a.pdf"},
]


def test_classify_and_order():
    assert listing.classify("Interim Financial Statements") == "interim"
    assert listing.classify("Errata to the Annual Report") == "errata"
    wl = listing.worklist(ROWS, {}, 100)
    assert [it["filing_id"] for it in wl] == [6, 8, 5, 7]     # tier 1 newest first, tier 2, then pre-window
    assert wl[0]["pdf_url"] == "https://cdn.cse.lk/upload_report_file/b.pdf"
    assert wl[0]["alt_urls"] == ["https://cdn.cse.lk/cmt/upload_report_file/b.pdf"]
    assert listing.worklist(ROWS, {"6": "ok"}, 2) == wl[1:3]


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_throttle_halves_and_recovers_and_breaks():
    c = Clock()
    th = fetch.Throttle(2, clock=c, sleep=c.sleep)
    for _ in range(3):
        th.acquire()
        th.release(True)
    th.acquire()
    th.release(False, "http_503", retry_after=10)
    th.acquire()
    th.release(False, "timeout")
    assert th.limit == 1 and th.pause >= 1 and th.stats["slowdowns"] == 1 and c.t >= 10
    for _ in range(fetch.WINDOW * 3):
        th.acquire()
        th.release(True)
    assert th.limit == 2 and th.pause == 0
    for _ in range(200):
        if th.open:
            break
        th.acquire()
        th.release(False, "http_429")
    assert th.open and th.stats["by_kind"]["http_429"] > 0
    with pytest.raises(fetch.CircuitOpen):
        th.acquire()


class Resp:
    def __init__(self, code, body=b"", headers=None):
        self.status_code, self.content, self.headers = code, body, headers or {}

    def close(self):
        pass


class Sess:
    def __init__(self, plan):
        self.plan, self.calls = plan, []

    def get(self, url, **kw):
        self.calls.append(url)
        return self.plan[url].pop(0)


def test_get_pdf_fallback_gone_and_retry(monkeypatch):
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)
    th = fetch.Throttle(2)
    s = Sess({"u": [Resp(403)], "alt": [Resp(503, headers={"Retry-After": "0"}), Resp(200, b"%PDF")]})
    assert fetch.get_pdf(s, th, ["u", "alt"]) == (b"%PDF", "alt", "ok")
    s = Sess({"u": [Resp(404)], "alt": [Resp(404)]})
    assert fetch.get_pdf(s, th, ["u", "alt"])[2] == "gone"
    s = Sess({"u": [Resp(500)] * 2})
    assert fetch.get_pdf(s, th, ["u"], attempts=2)[2] == "error"


def test_extract_layout(tmp_path):
    pdf = make_pdf(["Statement of profit or loss 1,234,567 revenue for the year ended 31 March 2019", ""])
    meta = extract.process({"filing_id": 42, "pdf_url": "https://x/y.pdf"}, pdf, str(tmp_path))
    d = tmp_path / "42"
    assert meta["status"] == "ok" and meta["pages_total"] == 2 and meta["engine"] == "pypdfium2"
    assert "profit or loss" in (d / "p0001.txt").read_text() and (d / "p0002.txt").read_text() == ""
    assert json.loads((d / "meta.json").read_text())["sha256"] == meta["sha256"]
    if shutil.which("tesseract"):
        assert meta["ocr_pages"] == [2] and (d / "ocr/p0002.txt").read_text().startswith("[OCR]\n")
    bad = extract.process({"filing_id": 43}, b"not a pdf", str(tmp_path))
    assert bad["status"] == "bad_pdf"


@pytest.mark.skipif(not shutil.which("age") or not shutil.which("age-keygen"), reason="age not installed")
def test_pack_encrypts_and_removes_plaintext(tmp_path):
    key = tmp_path / "k.txt"
    subprocess.run(["age-keygen", "-o", str(key)], check=True, capture_output=True)
    pub = re.search(r"age1[0-9a-z]+", key.read_text()).group(0)
    work, dist = tmp_path / "work", tmp_path / "dist"
    dist.mkdir()
    for fid in (1, 2):
        extract.process({"filing_id": fid}, make_pdf([f"page text {fid}"]), str(work))
    names = run._pack(work, [1, 2], dist, "bX-s0", pub)
    assert names == ["bX-s0-c0.tar.gz.age"] and not list(work.glob("*"))
    assert b"page text" not in (dist / names[0]).read_bytes()
    plain = subprocess.run(["age", "-d", "-i", str(key), str(dist / names[0])], check=True, capture_output=True).stdout
    with tarfile.open(fileobj=io.BytesIO(plain), mode="r:gz") as t:
        assert {"1/meta.json", "1/p0001.txt", "2/p0001.txt"} <= set(t.getnames())


SECRET_PATTERNS = [r"AGE-SECRET-KEY-", r"gh[pousr]_[A-Za-z0-9]{20,}", r"github_pat_", r"AKIA[0-9A-Z]{16}",
                   r"-----BEGIN [A-Z ]*PRIVATE KEY", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}",
                   r"(?i)(api[_-]?key|secret[_-]?access[_-]?key)\s*[:=]\s*['\"]?[A-Za-z0-9/+]{16,}"]


def test_repo_has_no_secrets_or_data():
    files = [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts and "__pycache__" not in p.parts
             and p.suffix not in (".lock",) and ".pytest_cache" not in p.parts]
    for p in files:
        text = p.read_text(errors="ignore")
        for pat in SECRET_PATTERNS:
            if p.name == "test_worker.py":
                continue
            assert not re.search(pat, text), f"{p.relative_to(ROOT)} matches {pat}"
        assert not p.suffix in (".pdf", ".sqlite", ".csv", ".gz", ".tar", ".age"), p


def test_workflows_only_schedule_and_dispatch_and_pinned():
    for wf in (ROOT / ".github/workflows").glob("*.yml"):
        text = wf.read_text()
        on = text.split("\non:", 1)[1].split("\n", 1)[1].split("\nconcurrency:", 1)[0].split("\npermissions:", 1)[0]
        triggers = set(re.findall(r"^  ([a-z_]+):", on, re.M))
        assert triggers <= {"schedule", "workflow_dispatch"}, (wf.name, triggers)
        assert re.search(r"^permissions:\n  contents: read$", text, re.M), wf.name
        for m in re.finditer(r"uses:\s*([^\s#]+)", text):
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", m.group(1)), m.group(1)
        assert "secrets." not in text and "pull_request" not in text, wf.name
