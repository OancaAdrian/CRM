# export_caen.py
import re
import csv
import unicodedata
import sys

# Dacă ai textul PDF deja extras într-un fișier .txt, setează TEXT_SOURCE la numele acelui fișier.
# Implicit scriptul caută un fișier "./caen_extracted.txt".
TEXT_SOURCE = "caen_extracted.txt"
OUT_CSV = "caen_rev2.csv"

def strip_diacritics(s: str) -> str:
    if s is None:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def load_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"File not found: {path}. You can paste the extracted text into that file and re-run.", file=sys.stderr)
        raise

def extract_pairs(text: str):
    """
    Heuristics:
    - Capturăm apariţii de coduri CAEN (3-4 cifre or 2 digits section codes) urmate de descriere
    - Formate posibile în textul HTML extras: '0111' în celulă + descriere în celulă următoare
    - Vom folosi regex care găseşte grupuri de cifre (2-4) care apar într-o coloană/tabel, apoi următoarea celulă de text.
    """
    # Normalize whitespace
    t = re.sub(r"\s+", " ", text)

    # Pattern: code like 4-digit or 3-digit or 2-digit (e.g., 0111, 4519, 01, 451)
    # We look for boundaries where code is separate token and following text has at least 3 non-digit chars (a description)
    pattern = re.compile(r"\b([0-9]{2,4})\b\s*[-:—]?\s*([A-ZÇȘȘA-Za-z0-9ăîâșțșĂÎÂȘȚçăâîşţ\w\s\-\(\)\,\.\/%&'’]+?)\s(?=(?:[0-9]{2,4}\b)|$)", re.IGNORECASE)

    matches = pattern.findall(t)
    pairs = []
    seen = set()
    for code, desc in matches:
        desc = desc.strip(" \t\n\r:;,.")
        if not desc:
            continue
        # skip entries where desc is just a section heading like "SECȚIUNEA A" — we keep only entries with at least 2 words
        if len(desc.split()) < 2:
            continue
        key = (code, desc)
        if key in seen:
            continue
        seen.add(key)
        pairs.append((code, desc))
    return pairs

def clean_description(s: str) -> str:
    s = s.strip()
    # remove excessive spaces around punctuation
    s = re.sub(r"\s*([,:;])\s*", r"\1 ", s)
    # unify multiple spaces
    s = re.sub(r"\s+", " ", s)
    # strip outer punctuation
    s = s.strip(" -–—:;,.")
    return s

def main():
    print("Loading text from", TEXT_SOURCE)
    raw = load_text(TEXT_SOURCE)
    pairs = extract_pairs(raw)
    if not pairs:
        print("No pairs found by heuristic. Trying alternative extraction...")

        # fallback: find patterns like '<td>0111</td> <td>Descriere</td>' from HTML-like extraction
        alt = re.findall(r">([0-9]{2,4})<[^>]*>\s*([^<]{3,200})<", raw)
        for code, desc in alt:
            desc = desc.strip()
            if len(desc.split()) < 2:
                continue
            if (code, desc) not in pairs:
                pairs.append((code, desc))

    # post-process: remove diacritics and normalize
    out_rows = []
    for code, desc in pairs:
        desc_clean = clean_description(desc)
        desc_unaccent = strip_diacritics(desc_clean)
        out_rows.append((code, desc_unaccent))

    # deduplicate by code, keep first description
    dedup = {}
    for code, desc in out_rows:
        if code not in dedup:
            dedup[code] = desc

    sorted_codes = sorted(dedup.items(), key=lambda x: (len(x[0]), x[0]))  # short codes first, then numeric

    # Write CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code","description"])
        for code, desc in sorted_codes:
            w.writerow([code, desc])

    print(f"Wrote {len(sorted_codes)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
