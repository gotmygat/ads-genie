from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from backend.actions import ActionExecutor
from backend.ads_client import GoogleAdsAdapter
from backend.config import Settings
from backend.db import Database
from backend.orchestrator import Orchestrator
from backend.reports import ReportService
from backend.slack_bridge import SlackBridge
from backend.tools import ToolEngine


class AdsGenieSystemTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "test.db")
        self.settings = Settings(
            app_host="127.0.0.1",
            app_port=0,
            db_path=db_path,
            timezone="America/Toronto",
            monitor_interval_seconds=300,
            enable_scheduler=False,
            auto_seed=True,
            google_ads_developer_token="",
            google_ads_client_id="",
            google_ads_client_secret="",
            google_ads_refresh_token="",
            google_ads_login_customer_id="",
            google_ads_api_version="v22",
            slack_bot_token="",
            slack_signing_secret="",
            slack_default_channel="",
            claude_api_key="",
        )

        self.db = Database(db_path)
        self.db.init_schema()
        self.db.seed_demo_data()
        self.ads = GoogleAdsAdapter(self.settings, self.db)
        self.tools = ToolEngine(self.db, self.ads)
        self.actions = ActionExecutor(self.db)
        self.reports = ReportService(self.db, self.tools, self.settings.timezone)
        self.orchestrator = Orchestrator(
            self.db,
            self.tools,
            self.actions,
            self.reports,
            SlackBridge(self.settings),
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


if __name__ == "__main__":
    unittest.main()
