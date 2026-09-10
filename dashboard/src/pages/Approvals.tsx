import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";

export function Approvals() {
  const { data, error, loading, refresh } = usePolling(() => api.listApprovals(true), 6000);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [rejectTarget, setRejectTarget] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [actionError, setActionError] = useState<string | null>(null);

  async function approve(incidentId: string) {
    setBusyId(incidentId);
    setActionError(null);
    try {
      await api.approveIncident(incidentId, "dashboard-operator");
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusyId(null);
    }
  }

  async function reject(incidentId: string) {
    if (!reason.trim()) {
      setActionError("A rejection reason is required.");
      return;
    }
    setBusyId(incidentId);
    setActionError(null);
    try {
      await api.rejectIncident(incidentId, "dashboard-operator", reason.trim());
      setRejectTarget(null);
      setReason("");
      refresh();
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div>
      <h1>Approvals</h1>
      <p className="page-subtitle">Pending human-in-the-loop approval requests, updated every 6s.</p>
      {error && <div className="error-banner">Failed to load approvals: {error}</div>}
      {actionError && <div className="error-banner">{actionError}</div>}
      {loading && !data && <div className="loading-text">Loading...</div>}
      {data && data.length === 0 && <div className="empty-text">No pending approvals.</div>}

      {data && data.length > 0 && (
        <div className="card">
          <table>
            <thead>
              <tr>
                <th>Incident</th>
                <th>Requested</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {data.map((a) => (
                <tr key={a.id} style={{ cursor: "default" }}>
                  <td>
                    <Link to={`/incidents/${a.incident_id}`} style={{ color: "var(--accent)" }}>
                      {a.incident_id.slice(0, 8)}…
                    </Link>
                  </td>
                  <td>{new Date(a.requested_at).toLocaleString()}</td>
                  <td>
                    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                      <button
                        className="primary"
                        disabled={busyId === a.incident_id}
                        onClick={() => approve(a.incident_id)}
                      >
                        Approve
                      </button>
                      <button
                        className="danger"
                        disabled={busyId === a.incident_id}
                        onClick={() =>
                          setRejectTarget(rejectTarget === a.incident_id ? null : a.incident_id)
                        }
                      >
                        Reject
                      </button>
                      {rejectTarget === a.incident_id && (
                        <div className="reject-form" style={{ marginTop: 0 }}>
                          <input
                            type="text"
                            placeholder="Reason (required)"
                            value={reason}
                            onChange={(e) => setReason(e.target.value)}
                          />
                          <button
                            className="danger"
                            disabled={busyId === a.incident_id}
                            onClick={() => reject(a.incident_id)}
                          >
                            Confirm
                          </button>
                        </div>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
