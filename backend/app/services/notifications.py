"""Notification service abstraction.

Two implementations selected by NOTIFICATION_CHANNEL (console|smtp, default
console):

- ConsoleNotifier: logs a structured, readable message via the existing JSON
  logger (app.observability.logging_config). This is the default and is
  what's actually exercised/verified in this repo -- no external
  credentials required.
- SMTPNotifier: a fully implemented real smtplib/email.mime sender that
  reads SMTP_HOST/PORT/USERNAME/PASSWORD/FROM_ADDR/TO_ADDR from
  app.config.settings. It is NOT the default and needs the user's own SMTP
  credentials to actually deliver mail -- it has not been exercised against
  a real mailbox in this session. See docs/safety-model.md.
"""
from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Protocol

from app.config import settings

logger = logging.getLogger("pipelinemedic.notifications")


class NotificationService(Protocol):
    def notify_incident_detected(self, incident: dict) -> None: ...

    def notify_approval_needed(self, incident: dict, approve_link_url: str) -> None: ...


class ConsoleNotifier:
    """Default notifier. Logs a structured, human-readable message through
    the standard JSON logger so it shows up in `docker compose logs backend`
    and is trivially greppable/verifiable in tests."""

    def notify_incident_detected(self, incident: dict) -> None:
        logger.info(
            "NOTIFICATION incident_detected: [%s/%s] %s (component=%s, incident_id=%s)",
            incident.get("severity"), incident.get("incident_type"), incident.get("title"),
            incident.get("source_component"), incident.get("id"),
        )

    def notify_approval_needed(self, incident: dict, approve_link_url: str) -> None:
        logger.info(
            "NOTIFICATION approval_needed: incident %s (%s) requires human approval -- "
            "review and decide at: %s",
            incident.get("id"), incident.get("title"), approve_link_url,
        )


class SMTPNotifier:
    """Real SMTP sender. Fully implemented; requires SMTP_HOST/PORT/USERNAME/
    PASSWORD/FROM_ADDR/TO_ADDR to be set via env (see .env.example) to
    actually send mail. If required settings are missing, logs an error and
    falls back to no-op rather than raising, so a misconfigured SMTP
    notifier cannot crash the agent graph."""

    def _send(self, subject: str, body: str) -> None:
        missing = [
            name for name, val in (
                ("SMTP_HOST", settings.smtp_host),
                ("SMTP_USERNAME", settings.smtp_username),
                ("SMTP_PASSWORD", settings.smtp_password),
                ("SMTP_FROM_ADDR", settings.smtp_from_addr),
                ("SMTP_TO_ADDR", settings.smtp_to_addr),
            )
            if not val
        ]
        if missing:
            logger.error(
                "SMTPNotifier misconfigured, cannot send email (missing: %s). "
                "Set these in .env -- see .env.example. Message dropped: %s",
                ", ".join(missing), subject,
            )
            return

        msg = MIMEMultipart()
        msg["From"] = settings.smtp_from_addr
        msg["To"] = settings.smtp_to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
                if settings.smtp_use_tls:
                    server.starttls()
                server.login(settings.smtp_username, settings.smtp_password)
                server.sendmail(settings.smtp_from_addr, [settings.smtp_to_addr], msg.as_string())
            logger.info("SMTPNotifier sent email: %s", subject)
        except Exception as e:  # pragma: no cover - requires real SMTP server
            logger.error("SMTPNotifier failed to send email %r: %s", subject, e)

    def notify_incident_detected(self, incident: dict) -> None:
        subject = f"[PipelineMedic] Incident detected: {incident.get('title')}"
        body = (
            f"Incident ID: {incident.get('id')}\n"
            f"Type: {incident.get('incident_type')}\n"
            f"Severity: {incident.get('severity')}\n"
            f"Component: {incident.get('source_component')}\n\n"
            f"{incident.get('description') or ''}\n"
        )
        self._send(subject, body)

    def notify_approval_needed(self, incident: dict, approve_link_url: str) -> None:
        subject = f"[PipelineMedic] Approval needed: {incident.get('title')}"
        body = (
            f"Incident ID: {incident.get('id')}\n"
            f"Type: {incident.get('incident_type')}\n"
            f"Severity: {incident.get('severity')}\n\n"
            f"Review and decide (approve/reject) here (link expires in "
            f"{settings.approval_token_max_age_seconds // 60} minutes, single use):\n"
            f"{approve_link_url}\n"
        )
        self._send(subject, body)


def get_notification_service() -> NotificationService:
    channel = (settings.notification_channel or "console").lower()
    if channel == "smtp":
        return SMTPNotifier()
    return ConsoleNotifier()
