"""Alert notifier for data-contract violations.

Reuses the same SMTP_* environment variables as quantFinance's email_service
(so a single .env configures both). Channels:

  - email     : SMTP to DATA_CONTRACT_ALERT_EMAIL (falls back to SMTP_FROM_EMAIL)
  - data_pulse: no-op here — surfacing happens by writing data_contract_state,
                which the data-pulse UI reads. Listed for completeness.
  - webhook   : optional POST to DATA_CONTRACT_WEBHOOK_URL (JSON payload)

The notifier never raises; failures are logged and reported in the return dict
so the reconciler stays resilient.
"""
from __future__ import annotations

import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any, Dict, List

_log = logging.getLogger(__name__)


class ContractNotifier:
    def __init__(self) -> None:
        self.smtp_host = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USERNAME", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.from_email = os.getenv("SMTP_FROM_EMAIL", self.smtp_user)
        self.from_name = os.getenv("FROM_NAME", "悟空量化数据合约")
        self.alert_email = os.getenv("DATA_CONTRACT_ALERT_EMAIL", self.from_email)
        self.webhook_url = os.getenv("DATA_CONTRACT_WEBHOOK_URL", "").strip()
        self.email_enabled = bool(self.smtp_user and self.smtp_password and self.alert_email)

    def notify(self, channels: List[str], subject: str, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Send an alert across the requested channels. Returns per-channel result."""
        results: Dict[str, Any] = {}
        for ch in channels:
            if ch == "email":
                results["email"] = self._send_email(subject, summary)
            elif ch == "webhook":
                results["webhook"] = self._post_webhook(subject, summary)
            elif ch == "data_pulse":
                results["data_pulse"] = "via_state"  # handled by state repository
            else:
                results[ch] = "unsupported"
        return results

    def _send_email(self, subject: str, summary: Dict[str, Any]) -> str:
        if not self.email_enabled:
            _log.warning("Contract email alert skipped: SMTP not configured")
            return "disabled"
        try:
            html = self._render_html(subject, summary)
            text = json.dumps(summary, ensure_ascii=False, indent=2, default=str)
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = formataddr((self.from_name, self.from_email))
            msg["To"] = self.alert_email
            msg.attach(MIMEText(text, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            _log.info("Contract alert email sent to %s", self.alert_email)
            return "sent"
        except Exception as e:  # pragma: no cover - network dependent
            _log.error("Contract alert email failed: %s", e)
            return f"error:{e}"

    def _post_webhook(self, subject: str, summary: Dict[str, Any]) -> str:
        if not self.webhook_url:
            return "disabled"
        try:
            import requests  # local import: optional dependency at call time

            resp = requests.post(
                self.webhook_url,
                json={"subject": subject, "summary": summary},
                timeout=float(os.getenv("DATA_CONTRACT_WEBHOOK_TIMEOUT", "5")),
            )
            return f"status:{resp.status_code}"
        except Exception as e:  # pragma: no cover - network dependent
            _log.error("Contract alert webhook failed: %s", e)
            return f"error:{e}"

    @staticmethod
    def _render_html(subject: str, summary: Dict[str, Any]) -> str:
        rows = "".join(
            f"<tr><td style='padding:4px 12px;color:#555'>{k}</td>"
            f"<td style='padding:4px 12px;font-family:monospace'>{v}</td></tr>"
            for k, v in summary.items()
        )
        return (
            f"<h3 style='margin:0 0 8px'>{subject}</h3>"
            "<table style='border-collapse:collapse;font-size:13px'>"
            f"{rows}</table>"
        )


__all__ = ["ContractNotifier"]
