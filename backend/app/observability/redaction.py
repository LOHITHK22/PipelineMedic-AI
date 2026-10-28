"""Best-effort secret redaction for data leaving the process boundary.

Used by the MCP server (app.mcp_server) on every tool result returned to an
MCP client and on every input/output/error string written to the audit log,
so that credentials that appear in backend error messages or log tails
(e.g. a libpq DSN in a psycopg2 error, `password=` lines in an Airflow task
log, a `user:pass@` URL) are not handed to an external AI client or
persisted verbatim.

This is pattern-based and therefore best-effort: it catches the common
credential shapes, not arbitrary sensitive business data (see
docs/mcp-tools.md "Known limitations").
"""
from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"

_SENSITIVE_KEY = re.compile(
    r"(pass(word|wd)?|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|authorization|cookie)",
    re.IGNORECASE,
)

_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # scheme://user:password@host  ->  scheme://user:[REDACTED]@host
    (re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.\-]*://[^:/@\s]+):([^@\s]+)@"), r"\1:" + REDACTED + "@"),
    # Authorization: Bearer xxx / Basic xxx
    (re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/]+=*"), r"\1 " + REDACTED),
    # password=xxx, api_key: "xxx", secret = 'xxx' (libpq DSNs, env dumps, config lines)
    (
        re.compile(
            r"(?i)\b([\w.\-]*(?:pass(?:word|wd)?|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key))"
            r"(\s*[=:]\s*)([\"']?)[^\s\"',;&]+"
        ),
        r"\1\2\3" + REDACTED,
    ),
]


def redact_text(text: str) -> str:
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def redact(value: Any) -> Any:
    """Return a copy of `value` (str / dict / list / scalars, arbitrarily
    nested) with credential-shaped strings masked and string values under
    sensitive-looking dict keys replaced. Non-string scalars are returned
    unchanged so typed result schemas stay valid."""
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if isinstance(k, str) and isinstance(v, str) and _SENSITIVE_KEY.search(k):
                out[k] = REDACTED
            else:
                out[k] = redact(v)
        return out
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return value
