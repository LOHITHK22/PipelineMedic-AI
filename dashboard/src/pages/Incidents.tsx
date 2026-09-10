import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { usePolling } from "../hooks/usePolling";
import { Badge } from "../components/Badge";
import type { Incident } from "../api/types";

const STATUS_OPTIONS = [
  "ALL",
  "DETECTED",
  "DIAGNOSING",
  "PLAN_READY",
  "AWAITING_APPROVAL",
  "EXECUTING",
  "VALIDATING",
  "RESOLVED",
  "ROLLED_BACK",
  "FAILED",
  "REJECTED",
];

const SEVERITY_OPTIONS = ["ALL", "LOW", "MEDIUM", "HIGH", "CRITICAL"];

type SortKey = "created_at" | "severity" | "status";

const SEVERITY_RANK: Record<string, number> = { LOW: 0, MEDIUM: 1, HIGH: 2, CRITICAL: 3 };

export function Incidents() {
  const { data, error, loading } = usePolling(() => api.listIncidents(), 7000);
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [severityFilter, setSeverityFilter] = useState("ALL");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("created_at");
  const [sortDesc, setSortDesc] = useState(true);
  const navigate = useNavigate();

  const filtered = useMemo(() => {
    if (!data) return [];
    let rows = data;
    if (statusFilter !== "ALL") rows = rows.filter((i) => i.status === statusFilter);
    if (severityFilter !== "ALL") rows = rows.filter((i) => i.severity === severityFilter);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      rows = rows.filter(
        (i) =>
          i.title.toLowerCase().includes(q) ||
          i.source_component.toLowerCase().includes(q) ||
          i.incident_type.toLowerCase().includes(q),
      );
    }
    const sorted = [...rows].sort((a, b) => {
      let cmp = 0;
      if (sortKey === "created_at") {
        cmp = (a.created_at ?? "").localeCompare(b.created_at ?? "");
      } else if (sortKey === "severity") {
        cmp = (SEVERITY_RANK[a.severity] ?? -1) - (SEVERITY_RANK[b.severity] ?? -1);
      } else {
        cmp = a.status.localeCompare(b.status);
      }
      return sortDesc ? -cmp : cmp;
    });
    return sorted;
  }, [data, statusFilter, severityFilter, search, sortKey, sortDesc]);

  function toggleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDesc((d) => !d);
    } else {
      setSortKey(key);
      setSortDesc(true);
    }
  }

  return (
    <div>
      <h1>Incidents</h1>
      <p className="page-subtitle">All incidents detected by PipelineMedic AI, updated every 7s.</p>
      {error && <div className="error-banner">Failed to load incidents: {error}</div>}

      <div className="toolbar">
        <input
          type="text"
          placeholder="Search title / component / type"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
          {STATUS_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s === "ALL" ? "All statuses" : s}
            </option>
          ))}
        </select>
        <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value)}>
          {SEVERITY_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s === "ALL" ? "All severities" : s}
            </option>
          ))}
        </select>
        <span style={{ color: "var(--text-dim)", fontSize: 12 }}>
          {filtered.length} of {data?.length ?? 0}
        </span>
      </div>

      {loading && !data && <div className="loading-text">Loading...</div>}
      {data && filtered.length === 0 && <div className="empty-text">No incidents match this filter.</div>}
      {filtered.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Component</th>
              <th>Type</th>
              <th onClick={() => toggleSort("severity")} style={{ cursor: "pointer" }}>
                Severity {sortKey === "severity" ? (sortDesc ? "↓" : "↑") : ""}
              </th>
              <th onClick={() => toggleSort("status")} style={{ cursor: "pointer" }}>
                Status {sortKey === "status" ? (sortDesc ? "↓" : "↑") : ""}
              </th>
              <th onClick={() => toggleSort("created_at")} style={{ cursor: "pointer" }}>
                Detected {sortKey === "created_at" ? (sortDesc ? "↓" : "↑") : ""}
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((i: Incident) => (
              <tr key={i.id} onClick={() => navigate(`/incidents/${i.id}`)}>
                <td>{i.title}</td>
                <td>{i.source_component}</td>
                <td>{i.incident_type}</td>
                <td>
                  <Badge label={i.severity} kind="severity" />
                </td>
                <td>
                  <Badge label={i.status} kind="status" />
                </td>
                <td>{i.created_at ? new Date(i.created_at).toLocaleString() : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
