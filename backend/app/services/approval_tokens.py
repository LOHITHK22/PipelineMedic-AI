"""Signed, single-use, time-limited approve-link tokens.

Mechanism: `itsdangerous.URLSafeTimedSerializer`. Chosen over a hand-rolled
HMAC scheme because it gives us, for free and audited: a signed payload
(tamper-evident -- flipping the incident_id or plan_id invalidates the
signature), a timestamp baked into the token so expiry checking doesn't need
its own DB column, and URL-safe base64 encoding so the token drops straight
into a query-free path segment for an email link. The added dependency
(`itsdangerous`) is a single, well-known, zero-transitive-dependency
library already used by Flask internally -- a reasonable trade for not
hand-rolling HMAC-with-expiry-and-encoding ourselves.

Payload: {"incident_id": ..., "plan_id": ..., "token_id": ...}. `token_id`
is a random opaque id (not secret) that the ApprovalToken DB row is keyed
on, so a signature can be cryptographically valid and not yet expired but
still rejected if that specific token_id has already been consumed or was
never issued (defense in depth: the DB is authoritative for single-use, the
signature is authoritative for tamper-evidence and expiry).

Security model: identical to a password-reset email link. There is no
separate authentication system in this repo (see docs/safety-model.md) --
possession of the (secret-signed, expiring, single-use) token IS the
credential. Anyone with the link can approve/reject exactly once, within
the expiry window, and only for the specific incident/plan it was minted
for.
"""
from __future__ import annotations

import secrets
from dataclasses import dataclass

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import ApprovalToken

_SALT = "pipelinemedic-approve-link"


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(settings.approval_token_secret, salt=_SALT)


@dataclass
class TokenValidationResult:
    valid: bool
    error: str | None = None
    incident_id: str | None = None
    plan_id: str | None = None
    token_row: ApprovalToken | None = None


def generate_approval_token(db: Session, incident_id: str, plan_id: str) -> str:
    """Mint a new signed, single-use, time-limited approve-link token and
    persist its (unconsumed) row. Returns the opaque token string to embed
    in the approve-link URL."""
    token_id = secrets.token_urlsafe(24)
    row = ApprovalToken(incident_id=incident_id, plan_id=plan_id, token_id=token_id, consumed=False)
    db.add(row)
    db.flush()  # assign row.id without requiring caller to commit yet

    payload = {"incident_id": incident_id, "plan_id": plan_id, "token_id": token_id}
    return _serializer().dumps(payload)


def validate_approval_token(db: Session, token: str, *, max_age_seconds: int | None = None) -> TokenValidationResult:
    """Validate signature + expiry + single-use, WITHOUT consuming the
    token. Safe to call from the read-only GET /approve-link/{token}
    endpoint any number of times."""
    max_age = max_age_seconds if max_age_seconds is not None else settings.approval_token_max_age_seconds
    try:
        payload = _serializer().loads(token, max_age=max_age)
    except SignatureExpired:
        return TokenValidationResult(valid=False, error="expired")
    except BadSignature:
        return TokenValidationResult(valid=False, error="invalid")

    token_id = payload.get("token_id")
    row = db.query(ApprovalToken).filter_by(token_id=token_id).first()
    if row is None:
        return TokenValidationResult(valid=False, error="invalid")
    if row.consumed:
        return TokenValidationResult(valid=False, error="already_used")

    return TokenValidationResult(
        valid=True,
        incident_id=payload["incident_id"],
        plan_id=payload["plan_id"],
        token_row=row,
    )


def consume_approval_token(db: Session, token: str) -> TokenValidationResult:
    """Validate then atomically mark the token consumed. Callers MUST check
    `.valid` before acting on the decision; a token that fails validation
    here is never marked consumed (so a caller can't accidentally burn a
    valid-but-unrelated token on a failed lookup)."""
    result = validate_approval_token(db, token)
    if not result.valid or result.token_row is None:
        return result

    from datetime import datetime

    result.token_row.consumed = True
    result.token_row.consumed_at = datetime.utcnow()
    db.flush()
    return result
