from __future__ import annotations

from datetime import datetime, timezone
from threading import Event, Thread
from zoneinfo import ZoneInfo
from typing import Any
import json
import time

from .actions import ActionExecutor
from .db import ActionRecord, Database
from .reports import ReportService
from .slack_bridge import SlackBridge
from .tools import ToolEngine


SEVERITY_ORDER = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

AUTONOMY_ORDER = {
    "auto_execute": 0,
    "propose_wait": 1,
    "draft_review": 2,
    "escalate": 3,
}


class Orchestrator:
    def __init__(
        self,
        db: Database,
        tools: ToolEngine,
        actions: ActionExecutor,
        reports: ReportService,
        slack: SlackBridge,
        timezone_name: str,
    ) -> None:
        self.db = db
        self.tools = tools
        self.actions = actions
        self.reports = reports
        self.slack = slack
        self.timezone = ZoneInfo(timezone_name)

    def _max_severity(self, severities: list[str]) -> str:
        if not severities:
            return "none"
        return max(severities, key=lambda s: SEVERITY_ORDER.get(s, 0))

    def _resolve_action_level(
        self,
        account: dict[str, Any],
        action_type: str,
        severity: str,
        health: dict[str, Any],
        roas_diag: dict[str, Any],
    ) -> str:
        autonomy = json.loads(account.get("autonomy_json", "{}") or "{}")
        action_levels = autonomy.get("action_levels", {})
        level = str(action_levels.get(action_type, autonomy.get("default", "propose_wait")))

        escalation = autonomy.get("escalation", {})
        spend_threshold = float(escalation.get("spend_anomaly_pct", 50))
        roas_threshold = float(escalation.get("roas_drop_pct", 45))

        spend_now = float(health["metrics"]["spend_7d"])
        spend_prev = float(health["metrics"].get("spend_prev_7d", health["metrics"]["spend_7d"]))
        spend_spike_pct = ((spend_now - spend_prev) / spend_prev * 100.0) if spend_prev > 0 else 0.0
        roas_drop = float(roas_diag["roas"].get("drop_pct", 0.0))

        if spend_spike_pct >= spend_threshold or roas_drop >= roas_threshold:
            return "escalate"

        # Guardrail: critical alerts never auto execute.
        if severity == "critical" and level == "auto_execute":
            return "propose_wait"

        return level

    def _build_recommendations(
        self,
        account: dict[str, Any],
        account_id: int,
        health: dict[str, Any],
        waste: dict[str, Any],
        roas_diag: dict[str, Any],
        negatives: dict[str, Any],
    ) -> list[dict[str, Any]]:
        campaigns = self.tools.ads_adapter.fetch_account_snapshot(account_id).campaigns
        recs: list[dict[str, Any]] = []
        is_live_account = str(account.get("data_source", "demo")).lower() == "live"

        candidate_keywords = negatives.get("keywords", [])[:8]

        # Keep live-account write actions conservative until full mutation wiring is enabled.
        if is_live_account:
            if candidate_keywords and float(waste["components"]["irrelevant_term_spend"]) >= 80:
                recs.append(
                    {
                        "action_type": "draft_campaign",
                        "risk": "medium",
                        "reason": "Live account: draft negative keywords for manual approval",
                        "params": {
                            "draft_type": "negative_keywords",
                            "keywords": candidate_keywords,
                            "note": "Apply these negatives directly in Google Ads until write mutations are enabled.",
                        },
                    }
                )

            roas_drop_pct = float(roas_diag["roas"]["drop_pct"])
            if health["severity"] in {"high", "critical"} and roas_drop_pct >= 25:
                draft = self.tools.draft_campaign(account_id, {"monthly_budget": 3000})
                recs.append(
                    {
                        "action_type": "draft_campaign",
                        "risk": "high",
                        "reason": "Live account: draft campaign for manual execution",
                        "params": draft,
                    }
                )
            return recs

        if candidate_keywords and float(waste["components"]["irrelevant_term_spend"]) >= 80:
            recs.append(
                {
                    "action_type": "add_negative_keywords",
                    "risk": "medium",
                    "reason": "Irrelevant search-term spend detected",
                    "params": {"keywords": candidate_keywords},
                }
            )

        zero_conv_candidates = sorted(
            [
                c
                for c in campaigns
                if float(c["conversions_7d"]) <= 0.0 and float(c["spend_7d"]) >= 250.0 and c["status"] == "enabled"
            ],
            key=lambda c: float(c["spend_7d"]),
            reverse=True,
        )
        if zero_conv_candidates:
            top = zero_conv_candidates[0]
            recs.append(
                {
                    "action_type": "pause_campaign",
                    "risk": "medium",
                    "reason": "High spend with zero conversions",
                    "params": {"campaign_id": int(top["id"]), "campaign_name": top["name"]},
                }
            )

        roas_drop_pct = float(roas_diag["roas"]["drop_pct"])
        if roas_drop_pct >= 15:
            spend_sorted = sorted(campaigns, key=lambda c: float(c["spend_7d"]), reverse=True)
            if spend_sorted:
                highest_spend = spend_sorted[0]
                recs.append(
                    {
                        "action_type": "adjust_bid",
                        "risk": "low",
                        "reason": "ROAS drop mitigation with conservative bid reduction",
                        "params": {
                            "campaign_id": int(highest_spend["id"]),
                            "campaign_name": highest_spend["name"],
                            "pct_delta": -8.0,
                        },
                    }
                )

        if health["severity"] in {"high", "critical"} and roas_drop_pct >= 25:
            draft = self.tools.draft_campaign(account_id, {"monthly_budget": 3000})
            recs.append(
                {
                    "action_type": "draft_campaign",
                    "risk": "high",
                    "reason": "Prepare replacement structure for manual review",
                    "params": draft,
                }
            )

        return recs

    def run_monitoring_cycle(self, account_id: int | None = None, triggered_by: str = "manual") -> dict[str, Any]:
        accounts = [self.db.get_account(account_id)] if account_id else self.db.list_accounts()
        accounts = [a for a in accounts if a]

        cross_mcc = self.tools.cross_mcc_anomalies(0, {})
        created_alerts = 0
        auto_executed = 0
        escalated = 0
        reviewed_clean = 0

        for account in accounts:
            aid = int(account["id"])
            health = self.tools.health_check(aid, {})
            waste = self.tools.analyze_budget_waste(aid, {})
            roas_diag = self.tools.diagnose_roas_drop(aid, {})
            benchmark = self.tools.benchmark_account(aid, {})
            negatives = self.tools.generate_negative_keywords(aid, {})

            overall_severity = self._max_severity(
                [health["severity"], waste["severity"], roas_diag["severity"]]
            )
            recommendations = self._build_recommendations(account, aid, health, waste, roas_diag, negatives)

            if overall_severity in {"none", "low"} and not recommendations:
                reviewed_clean += 1
                self.db.insert_decision(
                    account_id=aid,
                    actor="system",
                    action="health_check_no_action",
                    payload={
                        "triggered_by": triggered_by,
                        "health": health,
                        "waste": waste,
                        "roas": roas_diag,
                    },
                    alert_id=None,
                )
                continue

            if not recommendations:
                recommendations = [
                    {
                        "action_type": "draft_campaign",
                        "risk": "high",
                        "reason": "No safe automatic action identified",
                        "params": self.tools.draft_campaign(aid, {"monthly_budget": 3000}),
                    }
                ]

            levels = [
                self._resolve_action_level(account, rec["action_type"], overall_severity, health, roas_diag)
                for rec in recommendations
            ]
            overall_level = max(levels, key=lambda lvl: AUTONOMY_ORDER.get(lvl, 1))

            status = "open"
            requires_approval = True
            if overall_level == "auto_execute":
                status = "executed"
                requires_approval = False
            elif overall_level == "escalate":
                status = "escalated"
                requires_approval = True

            recommendation_payload = {
                "action": ", ".join(rec["action_type"] for rec in recommendations),
                "actions": recommendations,
            }
            context_payload = {
                "health_check": health,
                "budget_waste": waste,
                "roas_drop": roas_diag,
                "benchmark": benchmark,
                "cross_mcc": cross_mcc,
            }

            title = f"{account['name']} — {overall_severity.upper()} performance alert"
            summary = (
                f"ROAS {health['metrics']['roas_7d']}, waste ratio {waste['components']['waste_ratio']}, "
                f"ROAS drop {roas_diag['roas']['drop_pct']}%"
            )

            alert_id = self.db.insert_alert(
                account_id=aid,
                alert_type="monitoring",
                severity=overall_severity,
                status=status,
                autonomy_level=overall_level,
                requires_approval=requires_approval,
                title=title,
                summary=summary,
                recommendation=recommendation_payload,
                context=context_payload,
            )
            created_alerts += 1

            for rec, level in zip(recommendations, levels):
                action_status = "pending"
                action_reason = f"Autonomy level {level}; waiting for decision"
                if level == "draft_review":
                    action_status = "draft"
                elif level == "escalate":
                    action_status = "blocked"
                    action_reason = "Escalated by policy threshold"

                action_row = self.db.insert_action(
                    account_id=aid,
                    action_type=rec["action_type"],
                    params=rec["params"],
                    status=action_status,
                    reason=action_reason,
                    alert_id=alert_id,
                )

                if level == "auto_execute":
                    result = self.actions.execute(action_row, source="system")
                    auto_executed += 1 if result.get("ok") else 0
                elif level == "escalate":
                    escalated += 1

            decision_action = "alert_created"
            if overall_level == "auto_execute":
                decision_action = "auto_executed"
            elif overall_level == "escalate":
                decision_action = "escalated"

            self.db.insert_decision(
                account_id=aid,
                actor="system",
                action=decision_action,
                payload={
                    "alert_id": alert_id,
                    "autonomy_level": overall_level,
                    "triggered_by": triggered_by,
                    "recommendation": recommendation_payload,
                },
                alert_id=alert_id,
            )

            slack_result = self.slack.send_alert(account, self.db.get_alert(alert_id) or {})
            self.db.insert_decision(
                account_id=aid,
                actor="system",
                action="slack_notify",
                payload={"alert_id": alert_id, "result": slack_result},
                alert_id=alert_id,
            )

        return {
            "ok": True,
            "triggered_by": triggered_by,
            "processed_accounts": len(accounts),
            "alerts_created": created_alerts,
            "auto_executed_actions": auto_executed,
            "escalated_actions": escalated,
            "clean_accounts": reviewed_clean,
            "cross_mcc_anomalies": cross_mcc,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def apply_alert_decision(
        self,
        alert_id: int,
        decision: str,
        actor: str,
        modifications: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        alert = self.db.get_alert(alert_id)
        if not alert:
            raise ValueError(f"Alert {alert_id} not found")

        if decision not in {"approve", "dismiss", "modify"}:
            raise ValueError("Decision must be one of: approve, dismiss, modify")

        alert_actions = self.db.actions_for_alert(alert_id)
        account_id = int(alert["account_id"])
        modifications = modifications or {}
        executed = 0

        if decision == "dismiss":
            for raw in alert_actions:
                if raw["status"] in {"pending", "draft"}:
                    self.db.mark_action_status(int(raw["id"]), "blocked", "Dismissed by human")
            self.db.update_alert_status(alert_id, "dismissed")
            self.db.insert_decision(
                account_id=account_id,
                actor=actor,
                action="dismiss",
                payload={"alert_id": alert_id},
                alert_id=alert_id,
            )
            return {"ok": True, "alert_id": alert_id, "decision": "dismiss", "executed_actions": executed}

        # For modify, create replacement action when relevant.
        if decision == "modify":
            override_keywords = modifications.get("keywords")
            if override_keywords:
                self.db.insert_action(
                    account_id=account_id,
                    action_type="add_negative_keywords",
                    params={"keywords": list(override_keywords)},
                    status="pending",
                    reason="Human modification",
                    alert_id=alert_id,
                )
                alert_actions = self.db.actions_for_alert(alert_id)

        for raw in alert_actions:
            if raw["status"] not in {"pending", "draft"}:
                continue
            action = ActionRecord(
                id=int(raw["id"]),
                account_id=int(raw["account_id"]),
                action_type=str(raw["action_type"]),
                params=json.loads(raw["params_json"]),
                status=str(raw["status"]),
            )
            if decision == "modify":
                if action.action_type == "adjust_bid" and modifications.get("pct_delta") is not None:
                    action.params["pct_delta"] = float(modifications["pct_delta"])
                if action.action_type == "draft_campaign":
                    note = str(modifications.get("note", "")).strip()
                    if note:
                        action.params["human_note"] = note
                    if modifications.get("monthly_budget") is not None:
                        action.params["recommended_monthly_budget"] = float(modifications["monthly_budget"])
            result = self.actions.execute(action, source=actor)
            if result.get("ok"):
                executed += 1

        self.db.update_alert_status(alert_id, "executed")
        self.db.insert_decision(
            account_id=account_id,
            actor=actor,
            action=decision,
            payload={"alert_id": alert_id, "modifications": modifications, "executed": executed},
            alert_id=alert_id,
        )

        return {
            "ok": True,
            "alert_id": alert_id,
            "decision": decision,
            "executed_actions": executed,
        }


class SchedulerThread(Thread):
    def __init__(
        self,
        orchestrator: Orchestrator,
        reports: ReportService,
        db: Database,
        monitor_interval_seconds: int,
        timezone_name: str,
    ) -> None:
        super().__init__(daemon=True)
        self.orchestrator = orchestrator
        self.reports = reports
        self.db = db
        self.monitor_interval_seconds = max(30, monitor_interval_seconds)
        self.stop_event = Event()
        self.timezone = ZoneInfo(timezone_name)

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            now = datetime.now(self.timezone)

            last_monitor = self.db.get_scheduler_state("last_monitor_run")
            should_monitor = True
            if last_monitor:
                try:
                    last_dt = datetime.fromisoformat(last_monitor)
                    should_monitor = (now.timestamp() - last_dt.timestamp()) >= self.monitor_interval_seconds
                except ValueError:
                    should_monitor = True

            if should_monitor:
                self.orchestrator.run_monitoring_cycle(triggered_by="scheduler")
                self.db.set_scheduler_state("last_monitor_run", now.isoformat())

            weekly_key = f"weekly_{now:%Y-%m-%d}"
            if now.weekday() == 0 and now.hour == 8 and now.minute < 5:
                if self.db.get_scheduler_state(weekly_key) != "done":
                    self.reports.generate_weekly_mcc_report(now=now)
                    self.db.set_scheduler_state(weekly_key, "done")

            if now.day == 1 and now.hour == 8 and now.minute < 10:
                month_key = f"monthly_{now:%Y-%m}"
                if self.db.get_scheduler_state(month_key) != "done":
                    for account in self.db.list_accounts():
                        self.reports.generate_monthly_client_report(int(account["id"]), now=now)
                    self.db.set_scheduler_state(month_key, "done")

            self.stop_event.wait(5)
