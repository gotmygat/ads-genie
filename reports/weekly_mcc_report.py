from __future__ import annotations

from typing import Any
import json
import os

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment]

from mcp_server.tools.cross_mcc_anomalies import cross_mcc_anomalies
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.decision_log import DecisionLog
from slack_bot.handlers.report_handler import build_report_message
from slack_bot.messages.report_blocks import WeeklyReportPayload


class WeeklyReport(WeeklyReportPayload):
    pass


def _load_decisions() -> list[dict[str, Any]]:
    decision_log = DecisionLog()
    if decision_log.table is None:
        return []
    return decision_log.table.scan().get("Items", [])


def generate_weekly_mcc_report() -> WeeklyReport:
    anomalies = cross_mcc_anomalies().anomalies
    accounts = AccountRegistry().list_active_accounts()
    decisions = _load_decisions()
    resolved = [item for item in decisions if item.get("human_decision") in {"approved", "auto_executed"}]
    dismissed = [item for item in decisions if item.get("human_decision") in {"dismissed", "expired"}]
    top_issues = [f"{item.account_name}: {item.anomaly_type}" for item in anomalies[:10]]
    actions_taken = [f"{item.get('customer_id')}: {item.get('action_type')}" for item in resolved[:10]]
    spend_summary = {
        "accounts": len(accounts),
        "resolved_actions": len(resolved),
    }
    roas_trends = {
        "critical_anomalies": len([item for item in anomalies if item.severity in {"high", "critical"}]),
        "total_anomalies": len(anomalies),
    }
    return WeeklyReport(
        total_accounts_monitored=len(accounts),
        anomalies_detected=len(anomalies),
        anomalies_resolved=len(resolved),
        spend_summary=spend_summary,
        roas_trends=roas_trends,
        top_issues_by_account=top_issues,
        actions_taken=actions_taken,
        dismissed_or_expired=[f"{item.get('customer_id')}: {item.get('human_decision')}" for item in dismissed[:10]],
    )


def post_weekly_mcc_report(channel_id: str | None = None) -> dict[str, Any]:
    report = generate_weekly_mcc_report()
    channel = channel_id or os.getenv("MCC_SLACK_CHANNEL_ID", "")
    message = build_report_message(channel, report)
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if channel and token and WebClient is not None:
        WebClient(token=token).chat_postMessage(**message)
    return {"report": report.model_dump(mode="json"), "message": message}
