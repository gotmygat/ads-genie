from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json

from pydantic import BaseModel, Field


class AlertPayload(BaseModel):
    customer_id: str
    account_name: str
    alert_type: str
    root_cause_summary: str
    supporting_data: dict[str, Any] = Field(default_factory=dict)
    recommended_action: str
    confidence: str = "medium"
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    vertical: str | None = None
    slack_channel_id: str | None = None
    action_type: str | None = None
    execution_arn: str | None = None
    decision_id: str | None = None


def _metrics_table(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "No supporting metrics supplied."
    width = max(len(str(key)) for key in metrics) if metrics else 10
    lines = [f"{'Metric'.ljust(width)} | Value", f"{'-' * width} | -----"]
    for key, value in metrics.items():
        lines.append(f"{str(key).ljust(width)} | {value}")
    return "```" + "\n".join(lines) + "```"


def build_alert_blocks(alert: AlertPayload) -> list[dict[str, Any]]:
    action_value = json.dumps(
        {
            "customer_id": alert.customer_id,
            "action_type": alert.action_type,
            "execution_arn": alert.execution_arn,
            "decision_id": alert.decision_id,
        }
    )
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{alert.account_name} — {alert.alert_type}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": alert.root_cause_summary},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": _metrics_table(alert.supporting_data)},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Recommended action*\n{alert.recommended_action}"},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "action_id": "approve_action",
                    "value": action_value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Modify"},
                    "action_id": "modify_action",
                    "value": action_value,
                },
                {
                    "type": "button",
                    "style": "danger",
                    "text": {"type": "plain_text", "text": "Dismiss"},
                    "action_id": "dismiss_action",
                    "value": action_value,
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Alert generated at {alert.timestamp} | Confidence: {alert.confidence}",
                }
            ],
        },
    ]
