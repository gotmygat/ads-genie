from __future__ import annotations

import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen

from backend.actions import ActionExecutor
from backend.ads_client import GoogleAdsAdapter
from backend.config import Settings
from backend.db import Database
from backend.observability import RuntimeMonitor
from backend.orchestrator import Orchestrator
from backend.reports import ReportService
from backend.server import RequestHandler
from backend.slack_bridge import SlackBridge
from backend.tools import ToolEngine
from http.server import ThreadingHTTPServer


class AdsGenieSystemTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "test.db")
        self.settings = Settings(
            app_host="127.0.0.1",
            app_port=0,
            app_allowed_origins=("http://127.0.0.1:8080",),
            db_path=db_path,
            timezone="America/Toronto",
            environment="test",
            log_level="INFO",
            monitor_interval_seconds=300,
            enable_scheduler=False,
            auto_seed=True,
            app_auth_enabled=False,
            app_auth_username="admin",
            app_auth_password="",
            google_ads_developer_token="",
            google_ads_client_id="",
            google_ads_client_secret="",
            google_ads_refresh_token="",
            google_ads_login_customer_id="",
            google_ads_api_version="v22",
            slack_bot_token="",
            slack_app_token="",
            slack_signing_secret="",
            slack_default_channel="",
            claude_api_key="",
        )

        self.db = Database(db_path)
        self.db.init_schema()
        self.db.seed_demo_data()
        self.monitor = RuntimeMonitor(self.db, service="ads-genie-test", environment="test")
        self.ads = GoogleAdsAdapter(self.settings, self.db, monitor=self.monitor)
        self.tools = ToolEngine(self.db, self.ads)
        self.actions = ActionExecutor(self.db, self.ads, self.monitor)
        self.reports = ReportService(self.db, self.tools, self.settings.timezone)
        self.orchestrator = Orchestrator(
            self.db,
            self.tools,
            self.actions,
            self.reports,
            SlackBridge(self.settings, self.db, self.monitor),
            self.monitor,
            self.settings.timezone,
        )

    def tearDown(self) -> None:
        self.db.close()
        self.temp_dir.cleanup()

    def test_health_check_has_required_metrics(self) -> None:
        first_account = self.db.list_accounts()[0]
        self.assertEqual(first_account["data_source"], "demo")
        result = self.tools.health_check(int(first_account["id"]), {})
        self.assertIn("metrics", result)
        self.assertIn("roas_7d", result["metrics"])
        self.assertIn("risk_score", result)

    def test_monitoring_creates_alerts(self) -> None:
        summary = self.orchestrator.run_monitoring_cycle(triggered_by="test")
        self.assertTrue(summary["alerts_created"] >= 1)
        alerts = self.db.list_alerts()
        self.assertTrue(len(alerts) >= 1)

    def test_approve_decision_executes_actions(self) -> None:
        self.orchestrator.run_monitoring_cycle(triggered_by="test")
        open_alerts = [a for a in self.db.list_alerts() if a["status"] in {"open", "escalated", "executed"}]
        self.assertTrue(open_alerts)
        alert = open_alerts[0]

        result = self.orchestrator.apply_alert_decision(
            alert_id=int(alert["id"]),
            decision="approve",
            actor="test_user",
            modifications={},
        )
        self.assertTrue(result["ok"])

        updated = self.db.get_alert(int(alert["id"]))
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "executed")

    def test_legacy_autonomy_levels_are_normalized_on_create(self) -> None:
        account = self.db.create_account(
            name="Legacy Account",
            customer_id="200-200-2001",
            vertical="dental",
            timezone_value="America/Toronto",
            slack_channel="#legacy",
            autonomy_json='{"default":"propose_wait","action_levels":{"draft_campaign":"draft_review"}}',
        )
        stored = self.db.get_account(int(account["id"]))
        self.assertIsNotNone(stored)
        autonomy = json.loads(stored["autonomy_json"])
        self.assertEqual(autonomy["default"], "propose_and_wait")
        self.assertEqual(autonomy["action_levels"]["draft_campaign"], "draft_and_review")

    def test_quiet_hours_defer_auto_execute_and_slack_notify(self) -> None:
        account = self.db.list_accounts()[0]
        local_now = self.orchestrator._account_now(account)
        start = local_now.hour
        end = (start + 1) % 24
        self.db.execute(
            "UPDATE accounts SET quiet_hours_start = ?, quiet_hours_end = ? WHERE id = ?",
            (start, end, int(account["id"])),
        )

        summary = self.orchestrator.run_monitoring_cycle(account_id=int(account["id"]), triggered_by="test")
        self.assertEqual(summary["auto_executed_actions"], 0)

        decision_actions = {item["action"] for item in self.db.list_decisions(limit=10)}
        self.assertIn("slack_notify_deferred", decision_actions)

        alert = self.db.list_alerts(limit=1)[0]
        self.assertEqual(alert["status"], "open")

    def test_rollback_reverts_executed_negative_keyword_action(self) -> None:
        account = self.db.list_accounts()[0]
        before = len(self.db.list_negative_keywords(int(account["id"])))
        self.orchestrator.run_monitoring_cycle(account_id=int(account["id"]), triggered_by="test")

        candidate_alert = next(
            alert for alert in self.db.list_alerts() if alert["status"] in {"open", "escalated", "executed"}
        )
        if candidate_alert["status"] != "executed":
            approve = self.orchestrator.apply_alert_decision(
                alert_id=int(candidate_alert["id"]),
                decision="approve",
                actor="test_user",
                modifications={},
            )
            self.assertTrue(approve["ok"])

        executed_alert = self.db.get_alert(int(candidate_alert["id"]))
        self.assertIsNotNone(executed_alert)
        self.assertEqual(executed_alert["status"], "executed")
        mid = len(self.db.list_negative_keywords(int(account["id"])))
        self.assertGreater(mid, before)

        result = self.orchestrator.apply_alert_decision(
            alert_id=int(candidate_alert["id"]),
            decision="rollback",
            actor="test_user",
            modifications={},
        )
        self.assertTrue(result["ok"])

        after = len(self.db.list_negative_keywords(int(account["id"])))
        self.assertEqual(after, before)
        updated = self.db.get_alert(int(candidate_alert["id"]))
        self.assertEqual(updated["status"], "rolled_back")

    def test_placeholder_tools_are_hidden_and_blocked(self) -> None:
        tool_names = {tool["name"] for tool in self.tools.list_tools()}
        self.assertNotIn("competitor_analysis", tool_names)
        with self.assertRaisesRegex(ValueError, "disabled until implemented"):
            self.tools.run_tool("competitor_analysis", account_id=1, params={})

    def test_campaign_draft_endpoints_persist_files_and_notifications(self) -> None:
        class _Context:
            pass

        context = _Context()
        context.settings = self.settings
        context.db = self.db
        context.monitor = self.monitor
        context.ads = self.ads
        context.tools = self.tools
        context.actions = self.actions
        context.reports = self.reports
        context.slack = SlackBridge(self.settings, self.db, self.monitor)
        context.orchestrator = self.orchestrator

        RequestHandler.context = context
        server = ThreadingHTTPServer(("127.0.0.1", 0), RequestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        port = int(server.server_address[1])

        try:
            account = self.db.list_accounts()[0]
            draft_request = Request(
                f"http://127.0.0.1:{port}/api/campaigns/draft",
                data=json.dumps(
                    {
                        "account_id": int(account["id"]),
                        "prompt": "Focus on high-intent local storage demand and avoid discount-heavy copy.",
                        "campaign_goal": "Lead generation",
                        "target_geography": "Toronto +25mi",
                        "monthly_budget": 4200,
                        "files": [
                            {
                                "name": "brief.md",
                                "type": "text/markdown",
                                "content_base64": "IyBCcmllZgpVc2UgcHJlbWl1bSBtZXNzYWdpbmcgYW5kIGV4Y2x1ZGUgam9iLXNlZWtlcnMu",
                            }
                        ],
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(draft_request) as response:
                created = json.load(response)

            self.assertTrue(created["ok"])
            draft_id = int(created["draft"]["id"])
            self.assertEqual(created["draft"]["files"][0]["filename"], "brief.md")
            self.assertIn("Operator brief", created["draft"]["context_summary"])

            with urlopen(f"http://127.0.0.1:{port}/api/campaigns/draft/{draft_id}") as response:
                fetched = json.load(response)
            self.assertEqual(int(fetched["draft"]["id"]), draft_id)
            self.assertEqual(fetched["draft"]["draft"]["context_signals"]["file_count"], 1)

            approve_request = Request(
                f"http://127.0.0.1:{port}/api/campaigns/draft/{draft_id}/approve",
                data=json.dumps({"actor": "test_user"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(approve_request) as response:
                approved = json.load(response)
            self.assertTrue(approved["execution_arn"].startswith("arn:aws:states:local:"))
            self.assertEqual(approved["draft"]["status"], "approved")

            with urlopen(f"http://127.0.0.1:{port}/api/notifications?limit=10") as response:
                notifications = json.load(response)
            titles = {item["title"] for item in notifications["notifications"]}
            self.assertIn(created["draft"]["draft"]["campaign_name"], titles)
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
