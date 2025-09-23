import streamlit as st
import pandas as pd
import requests
from datetime import date

API_URL = "https://crm-api-galati.onrender.com"  # sau "http://127.0.0.1:8000" local

st.set_page_config(page_title="CRM Galați", layout="wide")
st.title("CRM Galați — Motor de căutare, agendă și import CSV")

# Căutare firmă
st.subheader("Căutare firmă")
cui_or_name = st.text_input("Introduceți CUI sau fragment de denumire", value="")

if st.button("Caută"):
    try:
        q = cui_or_name.strip()
        resp = requests.get(f"{API_URL}/firme", params={"q": q}, timeout=10)
        if resp.ok:
            results = resp.json()
            if results:
                st.session_state.selected_firm = results[0]
                st.success(f"Selectată: {results[0]['denumire']} ({results[0]['cui']})")
            else:
                st.warning("⚠️ Firma nu a fost găsită în baza de date.")
        else:
            st.error(f"❌ Eroare API: {resp.status_code} {resp.text}")
    except Exception as e:
        st.error(f"❌ Eroare la interogarea API: {e}")

# Afișare firmă selectată
if "selected_firm" in st.session_state:
    firm = st.session_state.selected_firm
    st.markdown(f"**Firmă selectată:** {firm.get('denumire')}")
    st.markdown(f"- CUI: `{firm.get('cui')}`")
    st.markdown(f"- Județ: `{firm.get('adr_judet')}`")
    st.markdown(f"- Cifra afaceri: `{firm.get('cifra_afaceri')}` RON")

# Upload CSV
st.subheader("Import activități din CRM DASHBOard (CSV)")
upload_col1, upload_col2 = st.columns([2,1])

with upload_col1:
    uploaded_file = st.file_uploader("Încarcă fișier CSV (export CRM DASHBOard)", type=["csv", "txt"])
    delimiter = st.text_input("Delimitator CSV", value=",")
    encoding = st.selectbox("Encoding", ["utf-8-sig", "utf-8", "latin-1"], index=0)

with upload_col2:
    st.write("Mapare coloane (dacă header diferit, editează):")
    type_col = st.text_input("Coloana tip activitate", value="type")
    date_col = st.text_input("Coloana dată", value="date")
    comment_col = st.text_input("Coloana comentariu", value="comment")
    score_col = st.text_input("Coloana scor", value="score")
    date_format = st.text_input("Format dată (opțional)", value="%Y-%m-%d")

if uploaded_file is not None:
    try:
        content = uploaded_file.getvalue().decode(encoding)
    except Exception:
        content = uploaded_file.getvalue().decode("latin-1")
    df = pd.read_csv(pd.io.common.StringIO(content), sep=delimiter)
    st.write("Preview (primele 10 rânduri):")
    st.dataframe(df.head(10))

    st.markdown("Coloane detectate:")
    st.write(list(df.columns))

    # Select cui target
    target_cui = st.text_input("CUI țintă pentru import (sau lasă gol pentru firmă selectată)", value="")
    if not target_cui and "selected_firm" in st.session_state:
        target_cui = st.session_state.selected_firm["cui"]

    st.markdown("Opțiuni import")
    preview_rows = st.slider("Câte rânduri să previzualizeze și/sau importa (0 = toate)", 0, 200, 10)
    do_preview = st.checkbox("Previzualizare mapare rânduri", value=True)

    def map_row(row):
        mapped = {
            "type": row.get(type_col) if type_col in row else (row.get("type") or row.get("activity_type") or None),
            "date": row.get(date_col) if date_col in row else (row.get("date") or row.get("Data") or None),
            "comment": row.get(comment_col) if comment_col in row else (row.get("comment") or row.get("descriere") or None),
            "score": row.get(score_col) if score_col in row else (row.get("score") or None)
        }
        return mapped

    if do_preview:
        preview = []
        for _, r in df.head(preview_rows if preview_rows>0 else len(df)).iterrows():
            preview.append(map_row(r))
        st.write("Exemplu mapare:")
        st.json(preview[:10])

    # Import buttons
    col_import1, col_import2 = st.columns(2)
    with col_import1:
        if st.button("Importă în backend (POST /activitati/firma/{cui}/import_csv)"):
            if not target_cui:
                st.error("Trebuie să specifici CUI țintă sau să selectezi o firmă mai sus.")
            else:
                try:
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
                    data = {
                        "delimiter": delimiter,
                        "date_format": date_format,
                        "type_column": type_col,
                        "comment_column": comment_col,
                        "score_column": score_col,
                        "date_column": date_col
                    }
                    url = f"{API_URL}/activitati/firma/{target_cui}/import_csv"
                    resp = requests.post(url, files=files, data=data, timeout=60)
                    if resp.ok:
                        st.success(f"✅ Import reușit: {resp.json()}")
                    else:
                        st.error(f"❌ Eroare import: {resp.status_code} {resp.text}")
                except Exception as e:
                    st.error(f"❌ Eroare la upload: {e}")

    with col_import2:
        if st.button("Importă local prin POST /agenda (rând cu rând)"):
            if not target_cui:
                st.error("Trebuie să specifici CUI țintă sau să selectezi o firmă mai sus.")
            else:
                created = 0
                errors = []
                for _, r in df.iterrows():
                    mapped = map_row(r)
                    payload = {
                        "cui": target_cui,
                        "data": mapped["date"] or date.today().isoformat(),
                        "scor": mapped["score"],
                        "comentariu": mapped["comment"] or str(mapped["type"])
                    }
                    try:
                        resp = requests.post(f"{API_URL}/agenda", json=payload, timeout=10)
                        if resp.ok:
                            created += 1
                        else:
                            errors.append({"status": resp.status_code, "text": resp.text, "row": mapped})
                    except Exception as e:
                        errors.append({"error": str(e), "row": mapped})
                st.success(f"Created: {created}, Errors: {len(errors)}")
                if errors:
                    st.write(errors[:10])
