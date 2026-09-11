import { useState } from "react";
import { useParams } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "../components/Badge";

const ERROR_MESSAGES: Record<string, string> = {
  expired: "This approve-link has expired (links are valid for a limited time).",
  invalid: "This approve-link is invalid or malformed.",
  already_used: "This approve-link has already been used to make a decision.",
  already_decided: "This incident has already been decided through another channel.",
};

/**
 * Landing page for someone arriving directly from a notification link
 * (email/console), without necessarily having the main dashboard open.
 * There is no separate auth system in this repo -- the token itself is the
 * credential, the same security model as a password-reset email link (see
 * docs/safety-model.md). Rendered from GET /approve-link/{token}; decisions
 * go through POST /approve-link/{token}/decide, which reuses the exact same
 * approve/reject service-layer logic as the main dashboard.
 */
export function ApproveLink() {
  const { token } = useParams<{ token: string }>();
  const { data, error, loading, refresh } = usePolling(() => api.getApproveLink(token as string), 15000);
  const [decision, setDecision] = useState<"approve" | "reject" | null>(null);
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [result, setResult] = useState<string | null>(null);

  async function submitDecision(kind: "approve" | "reject") {
    if (kind === "reject" && !reason.trim()) {
      setActionError("A rejection reason is required.");
      setDecision("reject");
      return;
    }
    setBusy(true);
    setActionError(null);
    try {
      const res = (await api.decideApproveLink(token as string, kind, reason.trim() || undefined)) as {
        status?: string;
      };
      setResult(`Decision recorded: incident is now ${res.status ?? "updated"}.`);
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  if (loading && !data) return <div className="loading-text">Loading approval details...</div>;
  if (error && !data) return <div className="error-banner">Failed to load approve-link: {error}</div>;
  if (!data) return null;

  if (!data.valid) {
    const message = ERROR_MESSAGES[data.error ?? ""] ?? "This approve-link can no longer be used.";
    return (
      <div className="card" style={{ maxWidth: 560, margin: "40px auto" }}>
        <h1>Approve-link unavailable</h1>
        <p className="empty-text">{message}</p>
        {data.incident_status && (
          <p className="page-subtitle">Incident status: {data.incident_status}</p>
        )}
      </div>
    );
  }

  const incident = data.incident!;
  const plan = data.plan;
  const alreadyDecided = result !== null || incident.status !== "AWAITING_APPROVAL";

  return (
    <div style={{ maxWidth: 720, margin: "0 auto" }}>
      <h1>{incident.title}</h1>
      <p className="page-subtitle">
        {incident.incident_type} &middot; {incident.source_component}
      </p>
      <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
        <Badge label={incident.severity} kind="severity" />
        <Badge label={incident.status} kind="status" />
      </div>

      {result && <div className="card">{result}</div>}
      {actionError && <div className="error-banner">{actionError}</div>}

      <div className="card">
        <h2>Description</h2>
        <p style={{ margin: 0 }}>{incident.description}</p>
      </div>

      {incident.diagnosis && (
        <div className="card">
          <h2>Diagnosis</h2>
          <div className="kv-row">
            <span className="k">Root cause</span>
            <span className="v">{incident.diagnosis.root_cause}</span>
          </div>
          <div className="kv-row">
            <span className="k">Confidence</span>
            <span className="v">{Math.round(incident.diagnosis.confidence * 100)}%</span>
          </div>
          <div className="kv-row">
            <span className="k">Reasoning</span>
            <span className="v">{incident.diagnosis.reasoning}</span>
          </div>
        </div>
      )}

      {plan && (
        <div className="card">
          <h2>Proposed repair plan</h2>
          <div className="kv-row">
            <span className="k">Summary</span>
            <span className="v">{plan.plan_json.summary}</span>
          </div>
          <div className="kv-row">
            <span className="k">Risk level</span>
            <span className="v">
              <Badge label={plan.risk_level} kind="risk" />
            </span>
          </div>
          {plan.risk_rationale && (
            <div className="kv-row">
              <span className="k">Rationale</span>
              <span className="v">{plan.risk_rationale}</span>
            </div>
          )}
          <div style={{ marginTop: 10 }}>
            {plan.plan_json.actions.map((a, idx) => (
              <div key={idx} className="plan-action">
                <div className="tool-name">{a.tool_name}</div>
                <div style={{ margin: "4px 0", fontSize: 13 }}>{a.description}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {!alreadyDecided && (
        <div className="card">
          <h2>Decision</h2>
          <div style={{ display: "flex", gap: 8 }}>
            <button className="primary" disabled={busy} onClick={() => submitDecision("approve")}>
              Approve
            </button>
            <button className="danger" disabled={busy} onClick={() => setDecision("reject")}>
              Reject
            </button>
          </div>
          {decision === "reject" && (
            <div className="reject-form">
              <input
                type="text"
                placeholder="Reason for rejection (required)"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
              />
              <button className="danger" disabled={busy} onClick={() => submitDecision("reject")}>
                Confirm reject
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
