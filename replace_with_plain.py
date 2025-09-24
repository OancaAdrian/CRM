# replace_with_plain.py
import csv, sys

IN = "caen_extracted_nodiac.csv"   # intrare: CSV cu coloana denumire_plain deja calculată
OUT = "caen_extracted_final.csv"   # ieșire pregătită pentru import

with open(IN, newline='', encoding='utf-8') as fin:
    rdr = csv.DictReader(fin, delimiter=';')
    fn = rdr.fieldnames or []
    if "denumire_plain" not in fn:
        print("Eroare: coloana 'denumire_plain' nu există în", IN)
        sys.exit(1)
    # asigurăm ordinea coloanelor: păstrăm toate coloanele originale, dar setăm denumire=denumire_plain
    out_fields = fn.copy()
    # dacă nu există denumire în header, adăugăm
    if "denumire" not in out_fields:
        out_fields.insert(3, "denumire")
    with open(OUT, "w", newline='', encoding='utf-8') as fout:
        w = csv.DictWriter(fout, fieldnames=out_fields, delimiter=';', extrasaction='ignore')
        w.writeheader()
        for r in rdr:
            r["denumire"] = r.get("denumire_plain", "")
            w.writerow(r)

print("Wrote", OUT)
