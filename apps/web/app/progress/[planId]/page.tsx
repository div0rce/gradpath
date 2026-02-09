import Link from "next/link";

export default async function ProgressPage({ params }: { params: Promise<{ planId: string }> }) {
  const { planId } = await params;

  return (
    <main className="grid">
      <div className="card">
        <div className="label">Progress</div>
        <h2 style={{ marginTop: 6 }}>Plan {planId}</h2>
        <p style={{ color: "var(--muted)" }}>
          Chart rendering will consume `/v1/plans/{'{planId}'}/audit/latest`. Current scaffold keeps routing and data contracts in place.
        </p>
        <Link href={`/degree-tracker/${planId}`}>Open requirement status</Link>
      </div>
    </main>
  );
}
