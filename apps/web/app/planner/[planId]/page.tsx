import { apiGet } from "@/lib/api";

type Snapshot = {
  snapshot_id: string;
  source: string;
  synced_at: string;
};

export default async function PlannerPage({ params }: { params: Promise<{ planId: string }> }) {
  const { planId } = await params;
  let snapshot: Snapshot | null = null;
  try {
    snapshot = await apiGet<Snapshot>("/v1/catalog/snapshots/active");
  } catch {
    snapshot = null;
  }

  return (
    <main className="grid">
      <div className="card">
        <div className="label">Planner</div>
        <h2 style={{ marginTop: 6 }}>Plan {planId}</h2>
        <p style={{ color: "var(--muted)" }}>
          Grid editing and real-time validation are scaffolded in backend APIs. This page is the frontend entrypoint.
        </p>
        {snapshot ? (
          <p>
            Active snapshot: <strong>{snapshot.snapshot_id}</strong> ({snapshot.source})
          </p>
        ) : (
          <p className="badge warn">No active snapshot</p>
        )}
      </div>
    </main>
  );
}
