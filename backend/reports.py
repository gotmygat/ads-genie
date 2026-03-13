from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any

from .db import Database
from .tools import ToolEngine


class ReportService:
    def __init__(self, db: Database, tools: ToolEngine, timezone_name: str) -> None:
        self.db = db
        self.tools = tools
        self.timezone = ZoneInfo(timezone_name)

    def _fmt_date(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def generate_weekly_mcc_report(self, now: datetime | None = None) -> dict[str, Any]:
        now_local = (now or datetime.now(self.timezone)).astimezone(self.timezone)
        period_end = now_local.date()
        period_start = (now_local - timedelta(days=7)).date()

        accounts = self.db.list_accounts()
        lines: list[str] = []
        lines.append(f"# Weekly MCC Report ({self._fmt_date(now_local)})")
        lines.append("")
        lines.append(f"Period: {period_start.isoformat()} to {period_end.isoformat()}")
        lines.append("")

        critical = 0
        high = 0
        for account in accounts:
            account_id = int(account["id"])
            health = self.tools.health_check(account_id, {})
            waste = self.tools.analyze_budget_waste(account_id, {})
            roas = self.tools.diagnose_roas_drop(account_id, {})

            severity = health["severity"]
            if severity == "critical":
                critical += 1
            if severity in {"critical", "high"}:
                high += 1

            lines.append(f"## {account['name']} ({account['vertical']})")
            lines.append(f"- Health: {health['severity']} (score {health['risk_score']})")
            lines.append(f"- ROAS: {health['metrics']['roas_7d']} | CPA: {health['metrics']['cpa_7d']}")
            lines.append(f"- Waste ratio: {waste['components']['waste_ratio']}")
            lines.append(f"- ROAS drop: {roas['roas']['drop_pct']}%")
            lines.append("")

        lines.append("## Portfolio Summary")
        lines.append(f"- Accounts reviewed: {len(accounts)}")
        lines.append(f"- High/Critical health flags: {high}")
        lines.append(f"- Critical health flags: {critical}")
        lines.append("")

        content = "\n".join(lines)
        report_id = self.db.write_report(
            report_type="weekly_mcc",
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            content_markdown=content,
            account_id=None,
        )
        report = self.db.latest_report("weekly_mcc") or {}
        report["id"] = report_id
        return report

    def generate_monthly_client_report(self, account_id: int, now: datetime | None = None) -> dict[str, Any]:
        now_local = (now or datetime.now(self.timezone)).astimezone(self.timezone)
        month_start = now_local.replace(day=1)
        period_start = month_start.date().isoformat()
        period_end = now_local.date().isoformat()

        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")

        health = self.tools.health_check(account_id, {})
        waste = self.tools.analyze_budget_waste(account_id, {})
        benchmark = self.tools.benchmark_account(account_id, {})
        actions = self.db.list_actions(account_id=account_id, limit=20)
        decisions = [d for d in self.db.list_decisions(limit=200) if int(d["account_id"]) == account_id][:20]

        lines: list[str] = []
        lines.append(f"# Monthly Report: {account['name']}")
        lines.append("")
        lines.append(f"Period: {period_start} to {period_end}")
        lines.append("")
        lines.append("## Performance Snapshot")
        lines.append(f"- ROAS: {health['metrics']['roas_7d']}")
        lines.append(f"- CPA: {health['metrics']['cpa_7d']}")
        lines.append(f"- Health score: {health['risk_score']} ({health['severity']})")
        lines.append(f"- Waste ratio: {waste['components']['waste_ratio']}")
        lines.append(f"- Benchmark score: {benchmark['benchmark_score']}")
        lines.append("")

        lines.append("## Recent Actions")
        if not actions:
            lines.append("- No actions executed yet")
        else:
            for action in actions[:10]:
                lines.append(
                    f"- [{action['status']}] {action['action_type']} ({action.get('created_at', 'n/a')})"
                )
        lines.append("")

        lines.append("## Recent Decisions")
        if not decisions:
            lines.append("- No human/system decisions logged yet")
        else:
            for decision in decisions[:10]:
                lines.append(f"- {decision['actor']} -> {decision['action']} ({decision['created_at']})")

        content = "\n".join(lines)
        report_id = self.db.write_report(
            report_type="monthly_client",
            period_start=period_start,
            period_end=period_end,
            content_markdown=content,
            account_id=account_id,
        )
        report = self.db.latest_report("monthly_client", account_id=account_id) or {}
        report["id"] = report_id
        return report
