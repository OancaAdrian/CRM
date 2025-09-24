# remove_diacritics_csv.py
import csv, unicodedata, re, sys

IN = "caen_extracted.csv"
OUT = "caen_extracted_nodiac.csv"

def remove_diacritics(s: str) -> str:
    if not s: return ""
    nf = unicodedata.normalize("NFKD", s)
    no = "".join(ch for ch in nf if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", no).strip()

# read and rewrite
with open(IN, newline="", encoding="utf-8") as f_in, open(OUT, "w", newline="", encoding="utf-8") as f_out:
    reader = csv.DictReader(f_in, delimiter=";")
    # ensure header includes denumire and denumire_plain (if not, we create them)
    fieldnames = reader.fieldnames or []
    # normalize header names
    fn = [n.strip() for n in fieldnames]
    # ensure denumire exists
    if "denumire" not in fn:
        print("Input CSV missing 'denumire' column. Columns:", fn)
        sys.exit(1)
    # ensure denumire_plain present in output
    out_fields = fn.copy()
    if "denumire_plain" not in out_fields:
        out_fields.append("denumire_plain")
    writer = csv.DictWriter(f_out, fieldnames=out_fields, delimiter=";", extrasaction="ignore")
    writer.writeheader()
    for row in reader:
        orig = row.get("denumire","")
        row["denumire_plain"] = remove_diacritics(orig)
        # optional: if you want to also replace denumire with no-diacritics, uncomment:
        # row["denumire"] = remove_diacritics(orig)
        writer.writerow(row)

print(f"Wrote {OUT}")
