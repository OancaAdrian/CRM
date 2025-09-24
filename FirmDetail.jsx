import React, { useState, useEffect, useRef } from "react";

export default function FirmDetail({ firmId, apiBase = "" }) {
  const [firm, setFirm] = useState(null);
  const [activities, setActivities] = useState([]);
  const [loadingFirm, setLoadingFirm] = useState(false);
  const [savingActivity, setSavingActivity] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [activityText, setActivityText] = useState("");
  const fileRef = useRef();

  // debounce guard: prevent concurrent requests for firm details
  const fetchFirm = async (id) => {
    if (!id) return;
    if (loadingFirm) return;
    setLoadingFirm(true);
    try {
      const res = await fetch(`${apiBase}/api/firms/${encodeURIComponent(id)}`);
      if (!res.ok) throw new Error(`Fetch firm failed: ${res.status}`);
      const data = await res.json();
      // replace state (no append)
      setFirm(data);
      // set activities from server (replace)
      setActivities(Array.isArray(data.activities) ? dedupeActivities(data.activities) : []);
    } catch (err) {
      console.error(err);
      // optional: show toast
    } finally {
      setLoadingFirm(false);
    }
  };

  useEffect(() => {
    fetchFirm(firmId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [firmId]);

  // dedupe helper (by unique id or CUI)
  function dedupeActivities(arr) {
    const map = new Map();
    for (const a of arr) {
      const key = a.id ?? `${a.type}::${a.text}::${a.created_at ?? ""}`;
      if (!map.has(key)) map.set(key, a);
    }
    return Array.from(map.values());
  }

  async function saveActivity() {
    if (!firmId || !activityText.trim()) return;
    if (savingActivity) return;
    setSavingActivity(true);
    try {
      const payload = { firm_id: firmId, type: "telefon", text: activityText.trim() };
      const res = await fetch(`${apiBase}/api/activities`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.status === 409) {
        // already exists
        const existing = await res.json();
        setActivities((prev) => dedupeActivities([existing, ...prev]));
      } else if (!res.ok) {
        throw new Error(`Save failed: ${res.status}`);
      } else {
        const created = await res.json();
        setActivities((prev) => dedupeActivities([created, ...prev]));
        setActivityText("");
      }
    } catch (err) {
      console.error(err);
      // show UI error (toast/modal)
    } finally {
      setSavingActivity(false);
    }
  }

  async function uploadCsv() {
    const file = fileRef.current?.files?.[0];
    if (!file) return alert("Selectează fișier CSV");
    if (uploading) return;
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      const res = await fetch(`${apiBase}/api/caen/import`, {
        method: "POST",
        body: fd,
      });
      if (!res.ok) {
        const text = await res.text().catch(() => null);
        throw new Error(`Upload failed ${res.status}: ${text || res.statusText}`);
      }
      const data = await res.json();
      // optional: refresh CAEN list or notify success
      alert("Import reușit: " + (data.imported_count ?? "OK"));
    } catch (err) {
      console.error(err);
      alert("Eroare la import: " + err.message);
    } finally {
      setUploading(false);
    }
  }

  return (
    <div>
      <button disabled={loadingFirm} onClick={() => fetchFirm(firmId)}>
        {loadingFirm ? "Se încarcă..." : "Încarcă firmă"}
      </button>

      {firm && (
        <div key={firm.cui ?? firm.id}>
          <h2>{firm.name}</h2>
          <div>CUI: {firm.cui}</div>
          <div>Județ: {firm.judet}</div>
          <div>Localitate: {firm.localitate}</div>
          <div>Cifră afaceri: {firm.cifra_afaceri}</div>
          <div>Profit net: {firm.profit_net}</div>
          <div>Angajați: {firm.angajati}</div>
          <div>Licențe: {firm.licente}</div>
          <div>CAEN: {firm.caen ?? "— (descriere lipsă)"}</div>
        </div>
      )}

      <section>
        <h3>Activități</h3>
        {activities.length === 0 && <div>Nu sunt activități</div>}
        <ul>
          {activities.map((a) => (
            <li key={a.id ?? `${a.type}-${a.text}-${a.created_at}`}>{a.type}: {a.text}</li>
          ))}
        </ul>
        <div>
          <input value={activityText} onChange={(e) => setActivityText(e.target.value)} placeholder="telefon / bla bla" />
          <button disabled={savingActivity} onClick={saveActivity}>
            {savingActivity ? "Se salvează..." : "Adaugă"}
          </button>
        </div>
      </section>

      <section>
        <h3>Import CSV</h3>
        <input type="file" accept=".csv,text/csv" ref={fileRef} />
        <button disabled={uploading} onClick={uploadCsv}>
          {uploading ? "Se încarcă..." : "Import CSV"}
        </button>
      </section>
    </div>
  );
}
