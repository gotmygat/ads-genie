from __future__ import annotations

from typing import Any
from urllib.parse import urlencode
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request

from .config import Settings
from .db import Database
from .observability import RuntimeMonitor


class SlackBridge:
    def __init__(self, settings: Settings, db: Database, monitor: RuntimeMonitor) -> None:
        self.settings = settings
        self.db = db
        self.monitor = monitor

    def _api_call(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.slack_bot_token:
            return {"ok": False, "reason": "missing_slack_token"}

        request = urllib.request.Request(
            f"https://slack.com/api/{endpoint}",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {self.settings.slack_bot_token}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.URLError as exc:
            self.monitor.exception("slack_api_call_failed", exc, message=f"Slack API call failed: {endpoint}")
            return {"ok": False, "reason": str(exc)}

    def build_alert_blocks(self, account: dict[str, Any], alert: dict[str, Any]) -> list[dict[str, Any]]:
        recommendation = json.loads(alert.get("recommendation_json", "{}") or "{}")
        action_line = recommendation.get("action", "Review in dashboard")
        value = json.dumps({"alert_id": int(alert["id"])})
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{alert.get('title', 'Ads Genie Alert')}*\n{alert.get('summary', '')}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"Account: *{account.get('name', 'Unknown')}* | "
                            f"Severity: *{alert.get('severity', 'unknown')}* | "
                            f"Autonomy: *{alert.get('autonomy_level', 'unknown')}*"
                        ),
                    }
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Recommendation*\n{action_line}"},
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "style": "primary",
                        "text": {"type": "plain_text", "text": "Approve"},
                        "action_id": "ads_genie_approve",
                        "value": value,
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Modify"},
                        "action_id": "ads_genie_modify",
                        "value": value,
                    },
                    {
                        "type": "button",
                        "style": "danger",
                        "text": {"type": "plain_text", "text": "Dismiss"},
                        "action_id": "ads_genie_dismiss",
                        "value": value,
                    },
                ],
            },
        ]

    def _post_message(self, channel: str, text: str, blocks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"channel": channel, "text": text}
        if blocks:
            payload["blocks"] = blocks
        return self._api_call("chat.postMessage", payload)

    def send_alert(self, account: dict[str, Any], alert: dict[str, Any]) -> dict[str, Any]:
        channel = account.get("slack_channel") or self.settings.slack_default_channel
        if not channel:
            return {"ok": False, "reason": "missing_slack_channel"}

        title = alert.get("title", "Ads Genie Alert")
        summary = alert.get("summary", "")
        blocks = self.build_alert_blocks(account, alert)
        result = self._post_message(channel=channel, text=f"{title}\n{summary}", blocks=blocks)
        if result.get("ok") and result.get("ts"):
            self.db.save_slack_message(
                alert_id=int(alert["id"]),
                account_id=int(account["id"]),
                channel=str(channel),
                message_ts=str(result["ts"]),
                payload=result,
            )
        return result

    def send_report(self, channel: str, title: str, body_markdown: str) -> dict[str, Any]:
        text = f"{title}\n{body_markdown}"
        return self._post_message(channel=channel, text=text)

    def verify_signature(self, headers: dict[str, str], raw_body: bytes) -> bool:
        if not self.settings.slack_signing_secret:
            return False
        signature = headers.get("x-slack-signature", "")
        timestamp = headers.get("x-slack-request-timestamp", "")
        if not signature or not timestamp:
            return False
        try:
            ts_value = int(timestamp)
        except ValueError:
            return False
        if abs(time.time() - ts_value) > 60 * 5:
            return False

        basestring = f"v0:{timestamp}:{raw_body.decode('utf-8')}".encode("utf-8")
        expected = "v0=" + hmac.new(
            self.settings.slack_signing_secret.encode("utf-8"),
            basestring,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def open_modify_modal(self, trigger_id: str, alert_id: int, existing_value: str = "") -> dict[str, Any]:
        view = {
            "type": "modal",
            "callback_id": "ads_genie_modify_submit",
            "title": {"type": "plain_text", "text": "Modify Action"},
            "submit": {"type": "plain_text", "text": "Send"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": json.dumps({"alert_id": alert_id}),
            "blocks": [
                {
                    "type": "input",
                    "block_id": "instruction_block",
                    "label": {"type": "plain_text", "text": "Modification instruction"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "instruction_input",
                        "initial_value": existing_value,
                        "multiline": False,
                        "placeholder": {"type": "plain_text", "text": "e.g. only add exact negatives, reduce bid by 5%"},
                    },
                }
            ],
        }
        return self._api_call("views.open", {"trigger_id": trigger_id, "view": view})

    def update_alert_message(self, alert_id: int, text: str, blocks: list[dict[str, Any]]) -> dict[str, Any]:
        message = self.db.get_slack_message(alert_id)
        if not message:
            return {"ok": False, "reason": "missing_stored_message"}
        return self._api_call(
            "chat.update",
            {
                "channel": message["channel"],
                "ts": message["message_ts"],
                "text": text,
                "blocks": blocks,
            },
        )

    def build_resolution_blocks(self, title: str, detail: str, status: str) -> list[dict[str, Any]]:
        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{title}*\n{detail}"},
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Resolved in Ads Genie dashboard | Status: *{status}*",
                    }
                ],
            },
        ]

