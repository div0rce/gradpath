import Link from "next/link";

export default function HomePage() {
  return (
    <main className="grid" style={{ gap: 20 }}>
      <header className="card">
        <div className="label">GradPath</div>
        <h1 style={{ margin: "8px 0 0 0" }}>Rutgers Degree Planner</h1>
        <p style={{ color: "var(--muted)", marginTop: 8 }}>
          Snapshot-pinned planning, deterministic validation, and audit-safe progress tracking.
        </p>
      </header>

      <section className="grid cards">
        <Link href="/catalog" className="card">
          <div className="label">Catalog</div>
          <div className="value">Course Search</div>
        </Link>
        <Link href="/planner/demo-plan" className="card">
          <div className="label">Planner</div>
          <div className="value">Plan Grid</div>
        </Link>
        <Link href="/degree-tracker/demo-plan" className="card">
          <div className="label">Tracker</div>
          <div className="value">Requirement Status</div>
        </Link>
        <Link href="/progress/demo-plan" className="card">
          <div className="label">Progress</div>
          <div className="value">Audit Summary</div>
        </Link>
      </section>
    </main>
  );
}
