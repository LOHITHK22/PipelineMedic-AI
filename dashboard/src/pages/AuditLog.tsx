import { Link } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";

function actorKind(actor: string): string {
  if (actor.startsWith("human")) return "HUMAN";
  if (actor === "agent") return "AGENT";
  return "SYSTEM";
}

export function AuditLog() {
  const { data, error, loading } = usePolling(() => api.listAudit(300), 8000);

  return (
    <div>
      <h1>Audit Log</h1>
      <p className="page-subtitle">
        Chronological feed of every SYSTEM / AGENT / HUMAN action, updated every 8s.
      </p>
      {error && <div className="error-banner">Failed to load audit log: {error}</div>}
      {loading && !data && <div className="loading-text">Loading...</div>}
      {data && data.length === 0 && <div className="empty-text">No audit entries yet.</div>}

      {data && data.length > 0 && (
        <div className="card">
          <div className="audit-row" style={{ borderBottom: "1px solid var(--border)", fontWeight: 600, color: "var(--text-dim)", fontSize: 11, textTransform: "uppercase" }}>
            <span>Time</span>
            <span>Actor</span>
            <span>Action / Details</span>
            <span>Incident</span>
          </div>
          {data.map((entry) => (
            <div key={entry.id} className="audit-row">
              <span style={{ color: "var(--text-dim)" }}>
                {entry.created_at ? new Date(entry.created_at).toLocaleString() : "—"}
              </span>
              <span>
                <span className="actor-tag">{actorKind(entry.actor)}</span>
              </span>
              <span>
                <div style={{ fontWeight: 600 }}>{entry.action}</div>
                {Object.keys(entry.details ?? {}).length > 0 && (
                  <pre
                    style={{
                      margin: "4px 0 0",
                      fontSize: 11,
                      color: "var(--text-dim)",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                    }}
                  >
                    {JSON.stringify(entry.details)}
                  </pre>
                )}
              </span>
              <span>
                {entry.incident_id ? (
                  <Link to={`/incidents/${entry.incident_id}`} style={{ color: "var(--accent)" }}>
                    {entry.incident_id.slice(0, 8)}…
                  </Link>
                ) : (
                  "—"
                )}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
