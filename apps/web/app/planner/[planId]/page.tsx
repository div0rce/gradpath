"use client";

import { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";
const READY_TO_DRAFT_BANNER = "Plan returned to DRAFT due to edits.";

type CompletionStatus = "YES" | "IN_PROGRESS" | "NO" | "BLANK";

type PlanItem = {
  id: string;
  term_id: string;
  position: number;
  raw_input: string;
  plan_item_status: "DRAFT" | "VALID" | "INVALID";
  completion_status: CompletionStatus;
  validation_reason: string | null;
  validation_meta: { missingPrereqs?: string[] } | null;
};

type PlanDetail = {
  plan_id: string;
  name: string;
  certification_state: "DRAFT" | "READY" | "CERTIFIED";
  items: PlanItem[];
};

type Term = {
  id: string;
  campus: string;
  code: string;
  year: number;
  season: "FALL" | "WINTER" | "SPRING" | "SUMMER";
};

type ReadySuccess = {
  plan_id: string;
  certification_state: "READY";
};

type FinalizeSuccess = {
  plan_id: string;
  certification_state: "CERTIFIED";
};

type DraftCell = { rawInput: string; completionStatus: CompletionStatus };
type DraftMap = Record<string, DraftCell>;
type Blocker = { code: string; count?: number };

function cellKey(termId: string, position: number): string {
  return `${termId}:${position}`;
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });
  if (!res.ok) {
    const maybeJson = await res.json().catch(() => null);
    const detail = maybeJson?.detail;
    if (typeof detail === "string" && detail.trim()) {
      throw new Error(detail);
    }
    const text = maybeJson ? "" : await res.text();
    if (text.trim()) {
      throw new Error(text);
    }
    throw new Error(`Request failed (${res.status})`);
  }
  return (await res.json()) as T;
}

export default function PlannerPage() {
  const params = useParams<{ planId: string | string[] }>();
  const planIdRaw = params.planId;
  const planId = Array.isArray(planIdRaw) ? planIdRaw[0] : planIdRaw;
  const [loading, setLoading] = useState(true);
  const [plan, setPlan] = useState<PlanDetail | null>(null);
  const [terms, setTerms] = useState<Term[]>([]);
  const [drafts, setDrafts] = useState<DraftMap>({});
  const [error, setError] = useState<string | null>(null);
  const [blockers, setBlockers] = useState<Blocker[]>([]);
  const [banner, setBanner] = useState<string | null>(null);

  const isCertified = plan?.certification_state === "CERTIFIED";

  async function loadPlannerState(): Promise<PlanDetail | null> {
    if (!planId) return null;
    const [planData, termData] = await Promise.all([
      fetchJson<PlanDetail>(`/v1/plans/${planId}`),
      fetchJson<Term[]>(`/v1/plans/${planId}/terms`),
    ]);
    setPlan(planData);
    setTerms(termData);
    setDrafts((prev) => {
      const next = { ...prev };
      for (const item of planData.items) {
        next[cellKey(item.term_id, item.position)] = {
          rawInput: item.raw_input,
          completionStatus: item.completion_status,
        };
      }
      return next;
    });
    return planData;
  }

  useEffect(() => {
    if (!planId) return;
    let mounted = true;
    async function run() {
      setLoading(true);
      setError(null);
      try {
        await loadPlannerState();
      } catch (e) {
        if (mounted) setError(e instanceof Error ? e.message : "Failed to load planner");
      } finally {
        if (mounted) setLoading(false);
      }
    }
    void run();
    return () => {
      mounted = false;
    };
  }, [planId]);

  const rowCount = useMemo(() => {
    if (!terms.length) return 10;
    // Phase 2 contract: row count is global across terms for grid alignment.
    const maxPos = plan?.items.reduce((m, x) => Math.max(m, x.position), 0) ?? 0;
    return Math.max(10, maxPos);
  }, [terms, plan]);

  function findItem(termId: string, position: number): PlanItem | undefined {
    return plan?.items.find((i) => i.term_id === termId && i.position === position);
  }

  async function saveCell(termId: string, position: number): Promise<void> {
    if (!plan) return;
    setError(null);
    setBlockers([]);

    const key = cellKey(termId, position);
    const draft = drafts[key] ?? { rawInput: "", completionStatus: "BLANK" as CompletionStatus };
    const existing = findItem(termId, position);
    const itemId = existing?.id ?? crypto.randomUUID();
    const previousState = plan.certification_state;

    try {
      await fetchJson(`/v1/plans/${planId}/items/${itemId}`, {
        method: "PUT",
        body: JSON.stringify({
          term_id: termId,
          position,
          raw_input: draft.rawInput,
          completion_status: draft.completionStatus,
        }),
      });
      const refreshed = await loadPlannerState();
      if (previousState === "READY" && refreshed?.certification_state === "DRAFT") {
        setBanner(READY_TO_DRAFT_BANNER);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save item");
    }
  }

  async function markReady(): Promise<void> {
    setError(null);
    setBlockers([]);
    try {
      const res = await fetch(`${API_BASE}/v1/plans/${planId}:ready`, { method: "POST" });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = body?.detail ?? {};
        if (detail?.error_code === "PLAN_NOT_READY") {
          setBlockers(Array.isArray(detail.blockers) ? detail.blockers : []);
        }
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }
      const ready = body as ReadySuccess;
      setPlan((prev) => (prev ? { ...prev, certification_state: ready.certification_state } : prev));
      setBanner(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to mark ready");
    }
  }

  async function finalizePlan(): Promise<void> {
    setError(null);
    setBlockers([]);
    try {
      const res = await fetch(`${API_BASE}/v1/plans/${planId}/finalize`, { method: "POST" });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = body?.detail ?? {};
        if (detail?.error_code === "PLAN_NOT_READY") {
          setBlockers(Array.isArray(detail.blockers) ? detail.blockers : []);
        }
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }
      const finalized = body as FinalizeSuccess;
      setPlan((prev) => (prev ? { ...prev, certification_state: finalized.certification_state } : prev));
      setBanner(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to finalize");
    }
  }

  if (loading) {
    return (
      <main>
        <div className="card">Loading planner...</div>
      </main>
    );
  }

  if (!plan) {
    return (
      <main>
        <div className="card">Unable to load planner.</div>
      </main>
    );
  }

  return (
    <main className="grid">
      <div className="card">
        <div className="label">Planner</div>
        <h2 style={{ marginTop: 6 }}>{plan.name}</h2>
        <p>
          Plan <strong>{plan.plan_id}</strong> state: <strong>{plan.certification_state}</strong>
        </p>
        {plan.certification_state === "CERTIFIED" && (
          <p className="badge ok">CERTIFIED: plan is read-only.</p>
        )}
        {banner && <p className="badge warn">{banner}</p>}
        {error && <p style={{ color: "var(--accent)" }}>{error}</p>}
        {!!blockers.length && (
          <div>
            <div className="label">Blocking Reasons</div>
            <ul style={{ marginTop: 8 }}>
              {blockers.map((b) => (
                <li key={b.code}>
                  {b.code}
                  {typeof b.count === "number" ? ` (${b.count})` : ""}
                </li>
              ))}
            </ul>
          </div>
        )}
        <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
          <button
            onClick={() => void markReady()}
            disabled={isCertified}
            style={{ padding: "8px 12px", borderRadius: 8, border: "1px solid var(--border)" }}
          >
            READY
          </button>
          <button
            onClick={() => void finalizePlan()}
            disabled={isCertified}
            style={{ padding: "8px 12px", borderRadius: 8, border: 0, background: "var(--accent)", color: "white" }}
          >
            FINALIZE
          </button>
        </div>
      </div>

      <div className="card" style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid var(--border)" }}>Row</th>
              {terms.map((term) => (
                <th key={term.id} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid var(--border)" }}>
                  {term.code}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Array.from({ length: rowCount }, (_, idx) => idx + 1).map((position) => (
              <tr key={position}>
                <td style={{ padding: 8, borderBottom: "1px solid var(--border)" }}>{position}</td>
                {terms.map((term) => {
                  const item = findItem(term.id, position);
                  const key = cellKey(term.id, position);
                  const draft = drafts[key] ?? {
                    rawInput: item?.raw_input ?? "",
                    completionStatus: item?.completion_status ?? ("BLANK" as CompletionStatus),
                  };
                  return (
                    <td key={key} style={{ minWidth: 260, padding: 8, borderBottom: "1px solid var(--border)" }}>
                      <input
                        value={draft.rawInput}
                        disabled={isCertified}
                        onChange={(e) =>
                          setDrafts((prev) => ({
                            ...prev,
                            [key]: { ...draft, rawInput: e.target.value },
                          }))
                        }
                        placeholder="Course code or text"
                        style={{
                          width: "100%",
                          padding: 8,
                          borderRadius: 8,
                          border: "1px solid var(--border)",
                          marginBottom: 6,
                        }}
                      />
                      <select
                        value={draft.completionStatus}
                        disabled={isCertified}
                        onChange={(e) =>
                          setDrafts((prev) => ({
                            ...prev,
                            [key]: { ...draft, completionStatus: e.target.value as CompletionStatus },
                          }))
                        }
                        style={{ width: "100%", padding: 8, borderRadius: 8, border: "1px solid var(--border)" }}
                      >
                        <option value="BLANK">BLANK</option>
                        <option value="YES">YES</option>
                        <option value="IN_PROGRESS">IN_PROGRESS</option>
                        <option value="NO">NO</option>
                      </select>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 6 }}>
                        <span className={`badge ${item?.plan_item_status === "VALID" ? "ok" : "warn"}`}>
                          {item?.plan_item_status ?? "DRAFT"}
                        </span>
                        {/*
                          CERTIFIED plans are read-only in Phase 2 UI.
                          Backend still remains authoritative for mutation protections.
                        */}
                        <button
                          onClick={() => void saveCell(term.id, position)}
                          disabled={isCertified}
                          style={{ padding: "6px 10px", borderRadius: 8, border: "1px solid var(--border)" }}
                        >
                          Save
                        </button>
                      </div>
                      {item?.validation_reason && (
                        <p style={{ margin: "6px 0 0 0", fontSize: 12, color: "var(--warn)" }}>
                          {item.validation_reason}
                          {item.validation_meta?.missingPrereqs?.length
                            ? `: ${item.validation_meta.missingPrereqs.join(", ")}`
                            : ""}
                        </p>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </main>
  );
}
