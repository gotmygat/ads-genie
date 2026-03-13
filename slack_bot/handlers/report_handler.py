from __future__ import annotations

from typing import Any

from slack_bot.messages.report_blocks import WeeklyReportPayload, build_weekly_report_blocks


def build_report_message(channel: str, report: WeeklyReportPayload) -> dict[str, Any]:
    return {
        "channel": channel,
        "text": "Weekly MCC Report",
        "blocks": build_weekly_report_blocks(report),
    }
