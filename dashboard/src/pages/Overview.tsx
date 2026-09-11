import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import type { ApprovalListItem, Incident, PipelineHealth } from "../api/types";

const ACTIVE_STATUSES = new Set([
  "DETECTED",
  "DIAGNOSING",
  "PLAN_READY",
  "AWAITING_APPROVAL",
  "EXECUTING",
  "VALIDATING",
]);
const RESOLVED_LIKE = new Set(["RESOLVED", "ROLLED_BACK"]);

async function fetchOverviewData() {
  const [incidents, approvals, pipelineHealth] = await Promise.all([
    api.listIncidents(),
    api.listApprovals(true),
    api.pipelineHealth(),
  ]);
  return { incidents, approvals, pipelineHealth };
}

function computeStats(incidents: Incident[], approvals: ApprovalListItem[]) {
  const active = incidents.filter((i) => ACTIVE_STATUSES.has(i.status)).length;
  const resolved = incidents.filter((i) => RESOLVED_LIKE.has(i.status)).length;
  const failed = incidents.filter((i) => i.status === "FAILED" || i.status === "REJECTED").length;
  const attempted = resolved + failed;
  const successRate = attempted > 0 ? Math.round((resolved / attempted) * 100) : null;

  return {
    active,
    resolved,
    pending: approvals.length,
    successRate,
  };
}

export function Overview() {
  const { data, error, loading } = usePolling(fetchOverviewData, 7000);
  const navigate = useNavigate();

  return (
    <div>
      <h1>Overview</h1>
      <p className="page-subtitle">
        Live status derived from real incident and approval data — polling every 7s.
      </p>
      {error && <div className="error-banner">Failed to load overview: {error}</div>}
      {loading && !data && <div className="loading-text">Loading...</div>}
      {data && (
        <>
          {(() => {
            const stats = computeStats(data.incidents, data.approvals);
            const health = data.pipelineHealth;
            const statusLabel: Record<PipelineHealth["overall"], string> = {
              HEALTHY: "Healthy",
              DEGRADED: "Degraded",
              UNKNOWN: "Unknown",
            };
            const statusClass: Record<PipelineHealth["overall"], string> = {
              HEALTHY: "healthy",
              DEGRADED: "degraded",
              UNKNOWN: "unknown",
            };
            return (
              <div className="stat-grid">
                <div className="stat-card">
                  <div className="stat-label">Pipeline Status</div>
                  <div className={`stat-value ${statusClass[health.overall]}`}>
                    {statusLabel[health.overall]}
                  </div>
                  <div className="stat-sublabel">
                    Flink: {health.flink.state} · Airflow:{" "}
                    {health.airflow.latest_run_state ?? (health.airflow.error ? "unreachable" : "n/a")}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Active Incidents</div>
                  <div className="stat-value">{stats.active}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Resolved Incidents</div>
                  <div className="stat-value">{stats.resolved}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Pending Approvals</div>
                  <div className="stat-value">{stats.pending}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Repair Success Rate</div>
                  <div className="stat-value">
                    {stats.successRate === null ? "—" : `${stats.successRate}%`}
                  </div>
                </div>
              </div>
            );
          })()}

          <div className="card">
            <h2>Most Recent Incidents</h2>
            {data.incidents.length === 0 && <div className="empty-text">No incidents yet.</div>}
            {data.incidents.length > 0 && (
              <table>
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Type</th>
                    <th>Severity</th>
                    <th>Status</th>
                    <th>Detected</th>
                  </tr>
                </thead>
                <tbody>
                  {data.incidents.slice(0, 8).map((i) => (
                    <tr key={i.id} onClick={() => navigate(`/incidents/${i.id}`)}>
                      <td>{i.title}</td>
                      <td>{i.incident_type}</td>
                      <td>{i.severity}</td>
                      <td>{i.status}</td>
                      <td>{i.created_at ? new Date(i.created_at).toLocaleString() : "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </div>
  );
}
