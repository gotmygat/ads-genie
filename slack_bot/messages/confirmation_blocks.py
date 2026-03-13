from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_confirmation_blocks(
    *,
    action_type: str,
    target_label: str,
    google_ads_operation_ids: list[str],
    decision_log_url: str | None = None,
    timestamp: str | None = None,
) -> list[dict[str, Any]]:
    rendered_timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    op_ids = ", ".join(google_ads_operation_ids) if google_ads_operation_ids else "n/a"
    decision_line = f"\nDecision log: {decision_log_url}" if decision_log_url else ""
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Action Executed", "emoji": True},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Action*: {action_type}\n"
                    f"*Target*: {target_label}\n"
                    f"*Executed at*: {rendered_timestamp}\n"
                    f"*Google Ads operation IDs*: {op_ids}{decision_line}"
                ),
            },
        },
    ]
