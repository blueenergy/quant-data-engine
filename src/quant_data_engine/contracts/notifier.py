"""Alert notifier for data-contract violations.

Reuses the same SMTP_* environment variables as quantFinance's email_service
(so a single .env configures both). Channels:

  - email     : SMTP to DATA_CONTRACT_ALERT_EMAIL (falls back to SMTP_FROM_EMAIL).
                Note the cluster only allows outbound 80/443, so SMTP ports are
                unreachable from the pod; webhook is the working channel there.
  - data_pulse: no-op here — surfacing happens by writing data_contract_state,
                which the data-pulse UI reads. Listed for completeness.
  - webhook   : POST to DATA_CONTRACT_WEBHOOK_URL. WeCom and DingTalk group-bot
                URLs each get that bot's markdown schema; anything else gets a
                generic JSON body. For DingTalk's signed mode, set
                DATA_CONTRACT_WEBHOOK_SECRET and the timestamp/sign pair is
                appended automatically.

The notifier never raises; failures are logged and reported in the return dict
so the reconciler stays resilient.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import smtplib
import time
import urllib.parse
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
                self._signed_url(),
                json=self._webhook_payload(subject, summary),
                timeout=float(os.getenv("DATA_CONTRACT_WEBHOOK_TIMEOUT", "5")),
            )
            return self._webhook_result(resp)
        except Exception as e:  # pragma: no cover - network dependent
            _log.error("Contract alert webhook failed: %s", e)
            return f"error:{e}"

    @property
    def _webhook_kind(self) -> str:
        if "qyapi.weixin.qq.com" in self.webhook_url:
            return "wecom"
        if "oapi.dingtalk.com" in self.webhook_url:
            return "dingtalk"
        return "generic"

    def _webhook_payload(self, subject: str, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Shape the body for the receiver. Group bots each want their own schema."""
        kind = self._webhook_kind
        if kind == "wecom":
            return {"msgtype": "markdown", "markdown": {"content": self._render_wecom(subject, summary)}}
        if kind == "dingtalk":
            return {
                "msgtype": "markdown",
                "markdown": {"title": subject, "text": self._render_dingtalk(subject, summary)},
            }
        return {"subject": subject, "summary": summary}

    def _webhook_result(self, resp: Any) -> str:
        """Both bots answer 200 with an errcode in the body when they reject a message."""
        if self._webhook_kind == "generic":
            return f"status:{resp.status_code}"
        try:
            body = resp.json()
        except ValueError:
            return f"error:non-json response (status {resp.status_code})"
        errcode = body.get("errcode")
        if errcode:
            return f"error:{errcode} {body.get('errmsg', '')}".strip()
        return "sent"

    def _signed_url(self) -> str:
        """DingTalk's optional signing mode; returns the URL unchanged without a secret."""
        secret = os.getenv("DATA_CONTRACT_WEBHOOK_SECRET", "").strip()
        if self._webhook_kind != "dingtalk" or not secret:
            return self.webhook_url
        timestamp = str(round(time.time() * 1000))
        digest = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}\n{secret}".encode("utf-8"),
            hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(digest))
        return f"{self.webhook_url}&timestamp={timestamp}&sign={sign}"

    @staticmethod
    def _render_wecom(subject: str, summary: Dict[str, Any]) -> str:
        # WeCom markdown supports a small subset: bold, quotes and font colors.
        lines = [f"**{subject}**"]
        for k, v in summary.items():
            if k in ("status", "violations") and v:
                lines.append(f'> {k}: <font color="warning">{v}</font>')
            else:
                lines.append(f"> {k}: {v}")
        return "\n".join(lines)

    @staticmethod
    def _render_dingtalk(subject: str, summary: Dict[str, Any]) -> str:
        # DingTalk takes standard markdown and has no font-color extension.
        lines = [f"## {subject}", ""]
        for k, v in summary.items():
            if k in ("status", "violations") and v:
                lines.append(f"- **{k}**: **{v}**")
            else:
                lines.append(f"- {k}: {v}")
        return "\n".join(lines)

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
