# export_caen_from_pdf.py
# Usage: python export_caen_from_pdf.py path/to/CAEN-Rev.2_structura-completa.pdf
# Requires: pip install PyPDF2

import sys
import re
import csv
import unicodedata
from pathlib import Path
from PyPDF2 import PdfReader

OUT_CSV = "caen_rev2.csv"

def strip_diacritics(s: str) -> str:
    if s is None:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def pdf_to_text(path: Path) -> str:
    reader = PdfReader(str(path))
    parts = []
    for p in reader.pages:
        try:
            t = p.extract_text() or ""
        except Exception:
            t = ""
        parts.append(t)
    return "\n".join(parts)

def clean_description(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s*([,:;])\s*", r"\1 ", s)
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" -–—:;,.")
    return s

def extract_pairs_from_text(text: str):
    # Normalize whitespace
    t = re.sub(r"[ \t\r\f\v]+", " ", text)
    t = re.sub(r"\n+", " \n ", t)

    pairs = []
    seen = set()

    # Strategy A: look for 4/3/2 digit codes followed by description (common in CAEN)
    pattern = re.compile(r"\b([0-9]{2,4})\b[^\S\r\n]*[:\-–—]?[^\S\r\n]*([A-ZĂÂÎȘȚa-zăâîșț0-9][^0-9\n]{3,300}?)\s(?=(?:[0-9]{2,4}\b)|\n|$)", re.IGNORECASE)
    for m in pattern.finditer(t):
        code = m.group(1).strip()
        desc = m.group(2).strip()
        desc = clean_description(desc)
        if len(desc.split()) < 2:
            continue
        key = (code, desc)
        if key in seen:
            continue
        seen.add(key)
        pairs.append((code, desc))

    # Strategy B fallback: find patterns where code and description appear near each other with newline/columns
    if not pairs:
        # look for lines that start with code then description
        for line in text.splitlines():
            line = line.strip()
            m = re.match(r"^([0-9]{2,4})\s+(.{3,200})$", line)
            if m:
                code = m.group(1)
                desc = clean_description(m.group(2))
                if len(desc.split()) < 2:
                    continue
                if (code, desc) not in seen:
                    seen.add((code, desc))
                    pairs.append((code, desc))

    # Extra pass: try to capture td-like html artifacts (if PDF->text left tags)
    if not pairs:
        alt = re.findall(r">([0-9]{2,4})<[^>]*>\s*([^<]{3,200})<", text)
        for code, desc in alt:
            desc = clean_description(desc)
            if len(desc.split()) < 2:
                continue
            if (code, desc) not in seen:
                seen.add((code, desc))
                pairs.append((code, desc))

    return pairs

def dedupe_by_code(pairs):
    dedup = {}
    for code, desc in pairs:
        if code not in dedup:
            dedup[code] = desc
    return sorted(dedup.items(), key=lambda x: (len(x[0]), x[0]))

def main():
    if len(sys.argv) < 2:
        print("Usage: python export_caen_from_pdf.py path/to/CAEN-Rev.2_structura-completa.pdf")
        sys.exit(1)
    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print("PDF not found:", pdf_path)
        sys.exit(1)

    print("Extracting text from PDF:", pdf_path)
    text = pdf_to_text(pdf_path)
    if not text.strip():
        print("No text extracted from PDF. Try a different extraction tool.")
        sys.exit(1)

    print("Extracting CAEN code/description pairs...")
    pairs = extract_pairs_from_text(text)
    if not pairs:
        print("No pairs found with heuristics. Attempting looser extraction...")

        # loose approach: find any 2-4 digit token followed by at least 3 words on same line
        loose = []
        for line in text.splitlines():
            m = re.search(r"\b([0-9]{2,4})\b[^\S\r\n]+(.{5,200})$", line)
            if m:
                code = m.group(1)
                desc = clean_description(m.group(2))
                if len(desc.split()) < 2:
                    continue
                if (code, desc) not in loose:
                    loose.append((code, desc))
        pairs = loose

    if not pairs:
        print("Still no pairs found. Aborting.")
        sys.exit(1)

    # Normalize: remove diacritics from descriptions
    out_rows = []
    for code, desc in pairs:
        desc_unaccent = strip_diacritics(desc)
        out_rows.append((code, desc_unaccent))

    rows = dedupe_by_code(out_rows)

    # Write CSV
    out = Path(OUT_CSV)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["code", "description"])
        for code, desc in rows:
            w.writerow([code, desc])

    print(f"Wrote {len(rows)} rows to {out.resolve()}")

if __name__ == "__main__":
    main()
