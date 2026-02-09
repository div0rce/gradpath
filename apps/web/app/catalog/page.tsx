"use client";

import { useState } from "react";

type Course = { code: string; title: string; credits: number; active: boolean };

export default function CatalogPage() {
  const [q, setQ] = useState("");
  const [rows, setRows] = useState<Course[]>([]);
  const [error, setError] = useState<string | null>(null);

  async function runSearch() {
    setError(null);
    const base = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";
    const res = await fetch(`${base}/v1/catalog/courses/search?q=${encodeURIComponent(q)}`);
    if (!res.ok) {
      setError(await res.text());
      return;
    }
    setRows((await res.json()) as Course[]);
  }

  return (
    <main className="grid">
      <div className="card">
        <div className="label">Catalog Search</div>
        <h2 style={{ marginTop: 6 }}>Find Courses</h2>
        <div style={{ display: "flex", gap: 8 }}>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="e.g. 14:540"
            style={{ flex: 1, padding: 10, borderRadius: 10, border: "1px solid var(--border)" }}
          />
          <button onClick={runSearch} style={{ padding: "10px 14px", borderRadius: 10, border: 0, background: "var(--accent)", color: "white" }}>
            Search
          </button>
        </div>
        {error && <p style={{ color: "var(--accent)", marginTop: 10 }}>{error}</p>}
      </div>

      <div className="card">
        <div className="label">Results</div>
        <div className="grid" style={{ marginTop: 10 }}>
          {rows.map((c) => (
            <div key={c.code} style={{ borderBottom: "1px solid var(--border)", paddingBottom: 8 }}>
              <strong>{c.code}</strong> - {c.title} ({c.credits})
            </div>
          ))}
          {!rows.length && <span style={{ color: "var(--muted)" }}>No results yet.</span>}
        </div>
      </div>
    </main>
  );
}
