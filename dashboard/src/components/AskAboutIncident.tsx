import { useState } from "react";
import { api } from "../api/client";

/**
 * Natural-language "ask about this incident" box. IMPORTANT: this is NOT a
 * new agentic action surface -- POST /incidents/{id}/ask never calls an LLM
 * and never invokes a tool. It only reads and rephrases this incident's
 * already-computed diagnosis/repair-plan/evidence deterministically (see
 * backend/app/services/incident_qa.py). It cannot trigger any action or
 * affect real infrastructure.
 */
export function AskAboutIncident({ incidentId }: { incidentId: string }) {
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    if (!question.trim()) return;
    setBusy(true);
    setError(null);
    try {
      const res = await api.askAboutIncident(incidentId, question.trim());
      setAnswer(res.answer);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card">
      <h2>Ask about this incident</h2>
      <p style={{ fontSize: 12, color: "var(--text-dim)", marginTop: -6, marginBottom: 10 }}>
        Answers are generated from this incident's existing diagnosis and repair plan only -- no
        new AI reasoning or tool calls happen here.
      </p>
      <div style={{ display: "flex", gap: 8 }}>
        <input
          type="text"
          placeholder="e.g. why is this broken, or what's the fix"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
          }}
          style={{ flex: 1 }}
        />
        <button className="primary" disabled={busy || !question.trim()} onClick={submit}>
          Ask
        </button>
      </div>
      {error && <div className="error-banner">{error}</div>}
      {answer && (
        <pre className="evidence" style={{ marginTop: 12, whiteSpace: "pre-wrap" }}>
          {answer}
        </pre>
      )}
    </div>
  );
}
