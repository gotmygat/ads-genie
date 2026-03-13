from __future__ import annotations

from typing import Any
import json
import urllib.error
import urllib.request

from .config import Settings


class SlackBridge:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _post_message(self, channel: str, text: str, blocks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        if not self.settings.slack_bot_token:
            return {"ok": False, "reason": "missing_slack_token"}

        payload: dict[str, Any] = {
            "channel": channel,
            "text": text,
        }
        if blocks:
            payload["blocks"] = blocks

        request = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
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
            return {"ok": False, "reason": str(exc)}

    def send_alert(self, account: dict[str, Any], alert: dict[str, Any]) -> dict[str, Any]:
        recommendation = json.loads(alert.get("recommendation_json", "{}"))
        channel = account.get("slack_channel") or self.settings.slack_default_channel
        if not channel:
            return {"ok": False, "reason": "missing_slack_channel"}

        title = alert.get("title", "Ads Genie Alert")
        summary = alert.get("summary", "")
        action_line = recommendation.get("action", "Review in dashboard")

        text = (
            f"{title}\n"
            f"Severity: {alert.get('severity', 'unknown')}\n"
            f"{summary}\n"
            f"Recommendation: {action_line}"
        )

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{title}*\n{summary}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Severity: *{alert.get('severity', 'unknown')}* | Autonomy: *{alert.get('autonomy_level', 'unknown')}*",
                    }
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Recommendation: {action_line}"},
            },
        ]

        return self._post_message(channel=channel, text=text, blocks=blocks)

    def send_report(self, channel: str, title: str, body_markdown: str) -> dict[str, Any]:
        text = f"{title}\n{body_markdown}"
        return self._post_message(channel=channel, text=text)
