interface BadgeProps {
  label: string;
  kind?: "severity" | "status" | "risk" | "decision" | "neutral";
}

const SEVERITY_COLORS: Record<string, string> = {
  LOW: "#2f7d4f",
  MEDIUM: "#9c7a1f",
  HIGH: "#b25a17",
  CRITICAL: "#b93a3a",
};

const STATUS_COLORS: Record<string, string> = {
  DETECTED: "#5a6472",
  DIAGNOSING: "#5a6472",
  PLAN_READY: "#3d6fb4",
  AWAITING_APPROVAL: "#9c7a1f",
  EXECUTING: "#3d6fb4",
  VALIDATING: "#3d6fb4",
  RESOLVED: "#2f7d4f",
  ROLLED_BACK: "#8a4fb0",
  FAILED: "#b93a3a",
  REJECTED: "#b93a3a",
};

const DECISION_COLORS: Record<string, string> = {
  PENDING: "#9c7a1f",
  APPROVED: "#2f7d4f",
  REJECTED: "#b93a3a",
};

export function Badge({ label, kind = "neutral" }: BadgeProps) {
  const map =
    kind === "severity" || kind === "risk"
      ? SEVERITY_COLORS
      : kind === "status"
        ? STATUS_COLORS
        : kind === "decision"
          ? DECISION_COLORS
          : {};
  const color = map[label] ?? "#5a6472";
  return (
    <span
      className="badge"
      style={{ backgroundColor: `${color}22`, color, borderColor: `${color}55` }}
    >
      {label}
    </span>
  );
}
