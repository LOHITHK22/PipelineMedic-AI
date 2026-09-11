import type {
  ApprovalListItem,
  ApproveLinkResponse,
  AskAnswer,
  AuditLogEntry,
  Incident,
  IncidentDetail,
  PipelineHealth,
} from "./types";

export const API_BASE_URL: string =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body?.detail ?? detail;
    } catch {
      /* ignore body parse errors */
    }
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  listIncidents: (status?: string) =>
    request<Incident[]>(`/incidents${status ? `?status=${encodeURIComponent(status)}` : ""}`),

  getIncident: (id: string) => request<IncidentDetail>(`/incidents/${id}`),

  approveIncident: (id: string, decided_by: string, reason?: string) =>
    request(`/incidents/${id}/approve`, {
      method: "POST",
      body: JSON.stringify({ decided_by, reason: reason ?? null }),
    }),

  rejectIncident: (id: string, decided_by: string, reason: string) =>
    request(`/incidents/${id}/reject`, {
      method: "POST",
      body: JSON.stringify({ decided_by, reason }),
    }),

  listApprovals: (pendingOnly = true) =>
    request<ApprovalListItem[]>(`/approvals?pending_only=${pendingOnly}`),

  listAudit: (limit = 200) => request<AuditLogEntry[]>(`/audit?limit=${limit}`),

  getApproveLink: (token: string) => request<ApproveLinkResponse>(`/approve-link/${encodeURIComponent(token)}`),

  decideApproveLink: (token: string, decision: "approve" | "reject", reason?: string) =>
    request(`/approve-link/${encodeURIComponent(token)}/decide`, {
      method: "POST",
      body: JSON.stringify({ decision, reason: reason ?? null }),
    }),

  askAboutIncident: (id: string, question: string) =>
    request<AskAnswer>(`/incidents/${id}/ask`, {
      method: "POST",
      body: JSON.stringify({ question }),
    }),

  health: () => request<{ status: string; llm_provider: string }>("/health"),

  pipelineHealth: () => request<PipelineHealth>("/health/pipeline"),

  metricsText: async (): Promise<string> => {
    const res = await fetch(`${API_BASE_URL}/metrics`);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.text();
  },
};
