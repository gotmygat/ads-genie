from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WeeklyReportPayload(BaseModel):
    total_accounts_monitored: int
    anomalies_detected: int
    anomalies_resolved: int
    spend_summary: dict[str, Any] = Field(default_factory=dict)
    roas_trends: dict[str, Any] = Field(default_factory=dict)
    top_issues_by_account: list[str] = Field(default_factory=list)
    actions_taken: list[str] = Field(default_factory=list)
    dismissed_or_expired: list[str] = Field(default_factory=list)


def build_weekly_report_blocks(report: WeeklyReportPayload) -> list[dict[str, Any]]:
    issue_lines = report.top_issues_by_account or ["No major account issues recorded."]
    action_lines = report.actions_taken or ["No automated or approved actions were executed."]
    dismissed_lines = report.dismissed_or_expired or ["No dismissed or expired alerts recorded."]
    spend_line = ", ".join(f"{key}: {value}" for key, value in report.spend_summary.items()) or "Not available"
    roas_line = ", ".join(f"{key}: {value}" for key, value in report.roas_trends.items()) or "Not available"
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Weekly MCC Report", "emoji": True},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Accounts monitored*: {report.total_accounts_monitored}\n"
                    f"*Anomalies detected*: {report.anomalies_detected}\n"
                    f"*Anomalies resolved*: {report.anomalies_resolved}\n"
                    f"*Spend summary*: {spend_line}\n"
                    f"*ROAS trends*: {roas_line}"
                ),
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top issues by account*\n- " + "\n- ".join(issue_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Actions taken*\n- " + "\n- ".join(action_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Dismissed / expired alerts*\n- " + "\n- ".join(dismissed_lines)}},
    ]
