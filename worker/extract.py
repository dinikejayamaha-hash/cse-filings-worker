"""Per-page text of a PDF: the pypdfium2 text layer of every page (pdfplumber if pypdfium2 cannot open the file),
plus tesseract OCR for pages without a text layer.

Output layout for one filing (written into a directory, later packed and encrypted):
    <filing_id>/meta.json          filing id, url, sha256, page count, engine, OCR pages, status, errors
    <filing_id>/pNNNN.txt          text layer of page NNNN (1-indexed), every page, possibly empty
    <filing_id>/ocr/pNNNN.txt      "[OCR]\\n" + tesseract text, for each page whose text layer has < 50 characters
Text files are UTF-8 ("surrogatepass" so any string the PDF library returns round-trips exactly).
"""
from __future__ import annotations

import hashlib
import io
import json
import time
from pathlib import Path

BLANK = 50                  # a page with fewer non-space-trimmed characters counts as having no text layer
OCR_MAX_PAGES = 400
OCR_SCALE = 200 / 72        # 200 dpi
OCR_CONFIG = "--psm 6"
OCR_PAGE_TIMEOUT = 120


def texts_raw(pdf: bytes) -> tuple[dict[int, str], str]:
    try:
        import pypdfium2 as pdfium
        doc = pdfium.PdfDocument(pdf)
        out = {}
        for i in range(len(doc)):
            try:
                out[i + 1] = doc[i].get_textpage().get_text_range() or ""
            except Exception:  # noqa: BLE001
                out[i + 1] = ""
        return out, "pypdfium2"
    except Exception:  # noqa: BLE001
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf)) as d:
            return {i: (p.extract_text() or "") for i, p in enumerate(d.pages, 1)}, "pdfplumber"


def ocr_blank_pages(pdf: bytes, texts: dict[int, str], max_pages: int = OCR_MAX_PAGES,
                    deadline: float | None = None) -> tuple[dict[int, str], list[str]]:
    """OCR blank pages in page order; at most `max_pages` successful pages. Failed pages are left out."""
    out, errs = {}, []
    blanks = [i for i, t in texts.items() if len(t.strip()) < BLANK]
    if not blanks:
        return out, errs
    try:
        import pypdfium2 as pdfium
        import pytesseract
        doc = pdfium.PdfDocument(pdf)
    except Exception as e:  # noqa: BLE001
        return out, [f"ocr unavailable: {type(e).__name__}: {e}"[:200]]
    for i in blanks:
        if len(out) >= max_pages:
            break
        if deadline and time.time() > deadline:
            errs.append(f"ocr stopped at page {i}: time limit")
            break
        try:
            img = doc[i - 1].render(scale=OCR_SCALE).to_pil()
            out[i] = "[OCR]\n" + pytesseract.image_to_string(img, config=OCR_CONFIG, timeout=OCR_PAGE_TIMEOUT)
        except Exception as e:  # noqa: BLE001
            errs.append(f"ocr page {i}: {type(e).__name__}: {e}"[:200])
    return out, errs


def _w(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8", "surrogatepass"))


def process(item: dict, pdf: bytes, outdir: str, ocr_seconds: float = 900) -> dict:
    """Extract one filing into outdir/<filing_id>/. Returns its meta (also written as meta.json)."""
    t0 = time.time()
    fid = item["filing_id"]
    d = Path(outdir) / str(fid)
    meta = {"filing_id": fid, "pdf_url": item.get("url_used") or item.get("pdf_url"),
            "sha256": hashlib.sha256(pdf).hexdigest(), "bytes": len(pdf), "status": "ok", "errors": []}
    try:
        raw, engine = texts_raw(pdf)
    except Exception as e:  # noqa: BLE001
        meta.update(status="bad_pdf", errors=[f"{type(e).__name__}: {e}"[:300]], pages_total=0)
        _w(d / "meta.json", json.dumps(meta))
        return meta
    ocr, errs = ocr_blank_pages(pdf, raw, deadline=t0 + ocr_seconds)
    for i, t in raw.items():
        _w(d / f"p{i:04d}.txt", t)
    for i, t in ocr.items():
        _w(d / "ocr" / f"p{i:04d}.txt", t)
    meta.update(pages_total=len(raw), engine=engine, blank_pages=sum(1 for t in raw.values() if len(t.strip()) < BLANK),
                ocr_pages=sorted(ocr), errors=errs, seconds=round(time.time() - t0, 1))
    if not raw:
        meta["status"] = "bad_pdf"
    _w(d / "meta.json", json.dumps(meta))
    return meta
