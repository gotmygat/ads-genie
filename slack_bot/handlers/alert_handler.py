from __future__ import annotations

from typing import Any

from slack_bot.messages.alert_blocks import AlertPayload, build_alert_blocks


def build_alert_message(alert: AlertPayload) -> dict[str, Any]:
    return {
        "channel": alert.slack_channel_id,
        "text": f"{alert.account_name} — {alert.alert_type}",
        "blocks": build_alert_blocks(alert),
    }
