import { apiGet } from "@/lib/api";

type Audit = {
  has_unsupported_rules: boolean;
  summary: {
    satisfiedRequirements: number;
    pendingRequirements: number;
    missingRequirements: number;
    unknownRequirements: number;
    percentComplete: number;
  };
};

export default async function DegreeTrackerPage({ params }: { params: Promise<{ planId: string }> }) {
  const { planId } = await params;
  let audit: Audit | null = null;
  try {
    audit = await apiGet<Audit>(`/v1/plans/${planId}/audit/latest`);
  } catch {
    audit = null;
  }

  return (
    <main className="grid">
      <div className="card">
        <div className="label">Degree Tracker</div>
        <h2 style={{ marginTop: 6 }}>Plan {planId}</h2>
        {audit ? (
          <>
            <p>
              Completion: <strong>{(audit.summary.percentComplete * 100).toFixed(1)}%</strong>
            </p>
            <div className="grid cards">
              <div className="card"><div className="label">Satisfied</div><div className="value">{audit.summary.satisfiedRequirements}</div></div>
              <div className="card"><div className="label">Pending</div><div className="value">{audit.summary.pendingRequirements}</div></div>
              <div className="card"><div className="label">Missing</div><div className="value">{audit.summary.missingRequirements}</div></div>
              <div className="card"><div className="label">Unknown</div><div className="value">{audit.summary.unknownRequirements}</div></div>
            </div>
            {audit.has_unsupported_rules && <p className="badge warn">Unsupported rules present</p>}
          </>
        ) : (
          <p style={{ color: "var(--muted)" }}>No audit found yet.</p>
        )}
      </div>
    </main>
  );
}
