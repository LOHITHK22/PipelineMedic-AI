import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "../components/Badge";
import { AskAboutIncident } from "../components/AskAboutIncident";

export function IncidentDetail() {
  const { id } = useParams<{ id: string }>();
  const { data: incident, error, loading, refresh } = usePolling(
    () => api.getIncident(id as string),
    6000,
  );
  const [actionError, setActionError] = useState<string | null>(null);
  const [rejectReason, setRejectReason] = useState("");
  const [showReject, setShowReject] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function handleApprove(approvalId: string) {
    setBusy(true);
    setActionError(null);
    try {
      await api.approveIncident(id as string, "dashboard-operator");
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusy(false);
    }
    void approvalId;
  }

  async function handleReject() {
    if (!rejectReason.trim()) {
      setActionError("A rejection reason is required.");
      return;
    }
    setBusy(true);
    setActionError(null);
    try {
      await api.rejectIncident(id as string, "dashboard-operator", rejectReason.trim());
      setShowReject(null);
      setRejectReason("");
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  if (loading && !incident) return <div className="loading-text">Loading incident...</div>;
  if (error && !incident) return <div className="error-banner">Failed to load incident: {error}</div>;
  if (!incident) return null;

  const pendingApproval = incident.approvals.find((a) => a.decision === "PENDING");

  return (
    <div>
      <Link to="/incidents" className="back-link">
        ← Back to incidents
      </Link>
      <h1>{incident.title}</h1>
      <p className="page-subtitle">
        {incident.incident_type} · {incident.source_component} · correlation{" "}
        <code>{incident.correlation_id.slice(0, 8)}</code>
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
        <Badge label={incident.severity} kind="severity" />
        <Badge label={incident.status} kind="status" />
      </div>

      {actionError && <div className="error-banner">{actionError}</div>}

      <div className="detail-grid">
        <div>
          <div className="card">
            <h2>Description</h2>
            <p style={{ margin: 0 }}>{incident.description}</p>
          </div>

          <div className="card">
            <h2>Evidence</h2>
            <pre className="evidence">{JSON.stringify(incident.evidence, null, 2)}</pre>
          </div>

          {incident.diagnosis && (
            <div className="card">
              <h2>Diagnosis</h2>
              <div className="kv-row">
                <span className="k">Confidence</span>
                <span className="v">{Math.round(incident.diagnosis.confidence * 100)}%</span>
              </div>
              <div className="kv-row">
                <span className="k">Root cause</span>
                <span className="v">{incident.diagnosis.root_cause}</span>
              </div>
              <div className="kv-row">
                <span className="k">Affected components</span>
                <span className="v">{incident.diagnosis.affected_components.join(", ")}</span>
              </div>
              <div className="kv-row">
                <span className="k">Reasoning</span>
                <span className="v">{incident.diagnosis.reasoning}</span>
              </div>
              <div className="kv-row">
                <span className="k">Recommended action</span>
                <span className="v">{incident.diagnosis.recommended_action_summary}</span>
              </div>
            </div>
          )}

          {incident.plans.length > 0 && (
            <div className="card">
              <h2>Repair Plan(s)</h2>
              {incident.plans.map((p) => (
                <div key={p.id} style={{ marginBottom: 16 }}>
                  <div className="kv-row">
                    <span className="k">Summary</span>
                    <span className="v">{p.plan_json.summary}</span>
                  </div>
                  <div className="kv-row">
                    <span className="k">Risk level</span>
                    <span className="v">
                      <Badge label={p.risk_level} kind="risk" />
                    </span>
                  </div>
                  <div className="kv-row">
                    <span className="k">Autonomy decision</span>
                    <span className="v">{p.autonomy_decision}</span>
                  </div>
                  {p.risk_rationale && (
                    <div className="kv-row">
                      <span className="k">Rationale</span>
                      <span className="v">{p.risk_rationale}</span>
                    </div>
                  )}
                  <div className="kv-row">
                    <span className="k">Expected outcome</span>
                    <span className="v">{p.plan_json.expected_outcome}</span>
                  </div>
                  <div style={{ marginTop: 10 }}>
                    {p.plan_json.actions.map((a, idx) => (
                      <div key={idx} className="plan-action">
                        <div className="tool-name">{a.tool_name}</div>
                        <div style={{ margin: "4px 0", fontSize: 13 }}>{a.description}</div>
                        <pre className="evidence" style={{ margin: 0 }}>
                          {JSON.stringify(a.tool_input, null, 2)}
                        </pre>
                        <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 4 }}>
                          Reversible: {a.is_reversible ? "yes" : "no"}
                          {a.rollback_tool_name ? ` (rollback: ${a.rollback_tool_name})` : ""}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}

          <AskAboutIncident incidentId={incident.id} />

          {incident.executions.length > 0 && (
            <div className="card">
              <h2>Execution Result</h2>
              {incident.executions.map((e) => (
                <div key={e.id} style={{ marginBottom: 10 }}>
                  <div className="kv-row">
                    <span className="k">Status</span>
                    <span className="v">{e.status}</span>
                  </div>
                  {e.error && (
                    <div className="kv-row">
                      <span className="k">Error</span>
                      <span className="v">{e.error}</span>
                    </div>
                  )}
                  <pre className="evidence">{JSON.stringify(e.tool_calls, null, 2)}</pre>
                </div>
              ))}
            </div>
          )}

          {incident.validations.length > 0 && (
            <div className="card">
              <h2>Validation Result</h2>
              {incident.validations.map((v) => (
                <div key={v.id} style={{ marginBottom: 10 }}>
                  <div className="kv-row">
                    <span className="k">Passed</span>
                    <span className="v">{v.passed ? "yes" : "no"}</span>
                  </div>
                  <pre className="evidence">{JSON.stringify(v.checks, null, 2)}</pre>
                </div>
              ))}
            </div>
          )}
        </div>

        <div>
          <div className="card">
            <h2>Approval Status</h2>
            {incident.approvals.length === 0 && (
              <div className="empty-text">No approval requested for this incident.</div>
            )}
            {incident.approvals.map((a) => (
              <div key={a.id} style={{ marginBottom: 12 }}>
                <div className="kv-row">
                  <span className="k">Decision</span>
                  <span className="v">
                    <Badge label={a.decision} kind="decision" />
                  </span>
                </div>
                <div className="kv-row">
                  <span className="k">Requested</span>
                  <span className="v">{new Date(a.requested_at).toLocaleString()}</span>
                </div>
                {a.decided_at && (
                  <div className="kv-row">
                    <span className="k">Decided</span>
                    <span className="v">
                      {new Date(a.decided_at).toLocaleString()} by {a.decided_by}
                    </span>
                  </div>
                )}
                {a.reason && (
                  <div className="kv-row">
                    <span className="k">Reason</span>
                    <span className="v">{a.reason}</span>
                  </div>
                )}
              </div>
            ))}

            {pendingApproval && (
              <div style={{ marginTop: 10 }}>
                <div style={{ display: "flex", gap: 8 }}>
                  <button className="primary" disabled={busy} onClick={() => handleApprove(pendingApproval.id)}>
                    Approve
                  </button>
                  <button
                    className="danger"
                    disabled={busy}
                    onClick={() => setShowReject(showReject ? null : pendingApproval.id)}
                  >
                    Reject
                  </button>
                </div>
                {showReject && (
                  <div className="reject-form">
                    <input
                      type="text"
                      placeholder="Reason for rejection (required)"
                      value={rejectReason}
                      onChange={(e) => setRejectReason(e.target.value)}
                    />
                    <button className="danger" disabled={busy} onClick={handleReject}>
                      Confirm reject
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="card">
            <h2>Timeline</h2>
            {incident.events.length === 0 && (
              <div className="empty-text">No incident_events recorded yet.</div>
            )}
            {incident.events.length > 0 && (
              <div className="timeline">
                {incident.events.map((ev) => (
                  <div key={ev.id} className="timeline-item">
                    <div className="t-header">
                      {ev.event_type}
                      <span className="t-time">
                        {ev.created_at ? new Date(ev.created_at).toLocaleString() : ""}
                      </span>
                    </div>
                    {Object.keys(ev.payload ?? {}).length > 0 && (
                      <pre>{JSON.stringify(ev.payload, null, 2)}</pre>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
