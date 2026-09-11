// Types mirroring backend/app/models/schemas.py and backend/app/db/models.py.
// These match the actual JSON shapes returned by backend/app/api/incidents.py's
// _serialize_incident() and related endpoints — not the raw Pydantic Incident model
// (which is the pre-persistence detector payload), since the API always returns
// the persisted IncidentRow shape.

export type IncidentType =
  | "SCHEMA_DRIFT"
  | "KAFKA_LAG"
  | "AIRFLOW_FAILURE"
  | "FLINK_FAILURE"
  | "DATA_QUALITY"
  | "POISON_MESSAGE";

export type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";

export type IncidentStatus =
  | "DETECTED"
  | "DIAGNOSING"
  | "PLAN_READY"
  | "AWAITING_APPROVAL"
  | "EXECUTING"
  | "VALIDATING"
  | "RESOLVED"
  | "ROLLED_BACK"
  | "FAILED"
  | "REJECTED";

export type RiskLevel = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
export type AutonomyDecision = "AUTO_EXECUTE" | "APPROVAL_REQUIRED" | "BLOCKED";
export type ApprovalDecisionValue = "PENDING" | "APPROVED" | "REJECTED";

export interface Diagnosis {
  root_cause: string;
  confidence: number;
  affected_components: string[];
  reasoning: string;
  recommended_action_summary: string;
}

export interface RepairAction {
  tool_name: string;
  tool_input: Record<string, unknown>;
  description: string;
  is_reversible: boolean;
  rollback_tool_name?: string | null;
  rollback_input?: Record<string, unknown> | null;
}

export interface RepairPlanJson {
  incident_id?: string | null;
  summary: string;
  actions: RepairAction[];
  expected_outcome: string;
  estimated_risk_level: RiskLevel;
}

export interface IncidentPlan {
  id: string;
  plan_json: RepairPlanJson;
  risk_level: RiskLevel;
  risk_rationale: string | null;
  autonomy_decision: string;
}

export interface IncidentApproval {
  id: string;
  decision: ApprovalDecisionValue;
  requested_at: string;
  decided_at: string | null;
  decided_by: string | null;
  reason: string | null;
}

export interface IncidentExecution {
  id: string;
  status: string;
  tool_calls: unknown[];
  error: string | null;
  idempotency_key: string;
}

export interface IncidentValidation {
  id: string;
  passed: boolean;
  checks: Record<string, unknown>;
}

export interface IncidentEventItem {
  id: string;
  event_type: string;
  payload: Record<string, unknown>;
  created_at: string | null;
}

export interface Incident {
  id: string;
  incident_type: IncidentType | string;
  severity: Severity | string;
  status: IncidentStatus | string;
  source_component: string;
  title: string;
  description: string;
  evidence: Record<string, unknown>;
  diagnosis: Diagnosis | null;
  correlation_id: string;
  created_at: string | null;
  updated_at: string | null;
  resolved_at: string | null;
}

export interface IncidentDetail extends Incident {
  plans: IncidentPlan[];
  approvals: IncidentApproval[];
  executions: IncidentExecution[];
  validations: IncidentValidation[];
  events: IncidentEventItem[];
}

export interface ApprovalListItem {
  id: string;
  incident_id: string;
  plan_id: string;
  decision: ApprovalDecisionValue;
  requested_at: string;
}

export type ComponentHealthStatus = "HEALTHY" | "DEGRADED" | "UNKNOWN";

export interface FlinkHealth {
  job_id: string | null;
  job_name: string;
  state: string;
  restart_count: number;
  exceptions: string[];
  error: string | null;
  status: ComponentHealthStatus;
}

export interface AirflowHealth {
  dag_id: string;
  is_paused: boolean | null;
  latest_run_state: string | null;
  error: string | null;
  status: ComponentHealthStatus;
}

export interface PipelineHealth {
  overall: ComponentHealthStatus;
  flink: FlinkHealth;
  airflow: AirflowHealth;
}

export interface ApproveLinkResponse {
  valid: boolean;
  error?: string | null;
  incident_status?: string;
  incident?: IncidentDetail;
  plan?: IncidentPlan | null;
}

export interface AskAnswer {
  incident_id: string;
  question: string;
  answer: string;
}

export interface AuditLogEntry {
  id: string;
  correlation_id: string;
  incident_id: string | null;
  actor: string;
  action: string;
  details: Record<string, unknown>;
  created_at: string | null;
}
