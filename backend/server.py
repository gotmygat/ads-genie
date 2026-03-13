from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import json
import re

from .actions import ActionExecutor
from .ads_client import GoogleAdsAdapter
from .config import Settings, load_settings
from .db import Database
from .orchestrator import Orchestrator, SchedulerThread
from .reports import ReportService
from .slack_bridge import SlackBridge
from .tools import ToolEngine


ROOT_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = ROOT_DIR / "frontend"


class AppContext:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.db.init_schema()
        if settings.auto_seed:
            self.db.seed_demo_data()

        self.ads = GoogleAdsAdapter(settings, self.db)
        self.tools = ToolEngine(self.db, self.ads)
        self.actions = ActionExecutor(self.db)
        self.reports = ReportService(self.db, self.tools, settings.timezone)
        self.slack = SlackBridge(settings)
        self.orchestrator = Orchestrator(
            self.db,
            self.tools,
            self.actions,
            self.reports,
            self.slack,
            settings.timezone,
        )
        self.scheduler: SchedulerThread | None = None

    def start_scheduler(self) -> None:
        if not self.settings.enable_scheduler or self.scheduler is not None:
            return
        self.scheduler = SchedulerThread(
            orchestrator=self.orchestrator,
            reports=self.reports,
            db=self.db,
            monitor_interval_seconds=self.settings.monitor_interval_seconds,
            timezone_name=self.settings.timezone,
        )
        self.scheduler.start()

    def stop(self) -> None:
        if self.scheduler:
            self.scheduler.stop()
            self.scheduler.join(timeout=3)
        self.db.close()


class RequestHandler(BaseHTTPRequestHandler):
    context: AppContext

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep output concise.
        print(f"[ads-genie] {self.address_string()} - {fmt % args}")

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(raw)

    def _send_text(self, status: int, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
        raw = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _not_found(self) -> None:
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not Found"})

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/api/health":
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "mode": self.context.ads.mode,
                    "google_ads_configured": self.context.settings.has_google_ads_credentials,
                    "scheduler_enabled": self.context.settings.enable_scheduler,
                    "monitor_interval_seconds": self.context.settings.monitor_interval_seconds,
                },
            )
            return

        if path == "/api/google-ads/test":
            account_id_raw = query.get("account_id", [None])[0]
            customer_id_raw = query.get("customer_id", [None])[0]
            customer_id = customer_id_raw
            if account_id_raw and not customer_id_raw:
                account = self.context.db.get_account(int(account_id_raw))
                if account:
                    customer_id = str(account.get("customer_id", ""))
            result = self.context.ads.test_connection(customer_id=customer_id)
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if path == "/api/google-ads/customers":
            if not self.context.settings.has_google_ads_credentials:
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "customers": [],
                        "configured": False,
                        "message": "Google Ads credentials not configured",
                    },
                )
                return
            customers = self.context.ads.list_accessible_customers()
            self._send_json(HTTPStatus.OK, {"ok": True, "configured": True, "customers": customers})
            return

        if path == "/api/tools":
            self._send_json(HTTPStatus.OK, {"ok": True, "tools": self.context.tools.list_tools()})
            return

        if path == "/api/accounts":
            accounts = self.context.db.list_accounts()
            enriched = []
            for account in accounts:
                aid = int(account["id"])
                health = self.context.tools.health_check(aid, {})
                waste = self.context.tools.analyze_budget_waste(aid, {})
                account_copy = dict(account)
                account_copy["health"] = health
                account_copy["waste"] = waste
                enriched.append(account_copy)
            self._send_json(HTTPStatus.OK, {"ok": True, "accounts": enriched})
            return

        m = re.fullmatch(r"/api/accounts/(\d+)", path)
        if m:
            aid = int(m.group(1))
            account = self.context.db.get_account(aid)
            if not account:
                self._not_found()
                return
            campaigns = self.context.db.campaigns_for_account(aid)
            memory = self.context.db.list_context_memory(aid)
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "account": account,
                    "campaigns": campaigns,
                    "context_memory": memory,
                    "negatives": self.context.db.list_negative_keywords(aid),
                },
            )
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/campaigns", path)
        if m:
            aid = int(m.group(1))
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "campaigns": self.context.db.campaigns_for_account(aid)},
            )
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/negatives", path)
        if m:
            aid = int(m.group(1))
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "negative_keywords": self.context.db.list_negative_keywords(aid)},
            )
            return

        if path == "/api/alerts":
            status = query.get("status", [None])[0]
            alerts = self.context.db.list_alerts(status=status)
            for alert in alerts:
                alert["recommendation"] = json.loads(alert.get("recommendation_json", "{}"))
                alert["context"] = json.loads(alert.get("context_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "alerts": alerts})
            return

        if path == "/api/decisions":
            decisions = self.context.db.list_decisions(limit=300)
            for decision in decisions:
                decision["payload"] = json.loads(decision.get("payload_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "decisions": decisions})
            return

        if path == "/api/actions":
            actions = self.context.db.list_actions(limit=300)
            for action in actions:
                action["params"] = json.loads(action.get("params_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "actions": actions})
            return

        if path == "/api/reports/weekly/latest":
            report = self.context.db.latest_report("weekly_mcc")
            self._send_json(HTTPStatus.OK, {"ok": True, "report": report})
            return

        m = re.fullmatch(r"/api/reports/monthly/(\d+)/latest", path)
        if m:
            aid = int(m.group(1))
            report = self.context.db.latest_report("monthly_client", account_id=aid)
            self._send_json(HTTPStatus.OK, {"ok": True, "report": report})
            return

        m = re.fullmatch(r"/api/context/(\d+)", path)
        if m:
            aid = int(m.group(1))
            memories = self.context.db.list_context_memory(aid)
            self._send_json(HTTPStatus.OK, {"ok": True, "context_memory": memories})
            return

        # Static frontend files.
        if path in {"/", "/index.html"}:
            self._serve_frontend_file("index.html", "text/html; charset=utf-8")
            return
        if path == "/app.js":
            self._serve_frontend_file("app.js", "application/javascript; charset=utf-8")
            return
        if path == "/styles.css":
            self._serve_frontend_file("styles.css", "text/css; charset=utf-8")
            return

        self._not_found()

    def _serve_frontend_file(self, filename: str, content_type: str) -> None:
        target = FRONTEND_DIR / filename
        if not target.exists():
            self._send_text(HTTPStatus.NOT_FOUND, "Missing frontend asset")
            return
        self._send_text(HTTPStatus.OK, target.read_text(encoding="utf-8"), content_type=content_type)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/accounts":
            try:
                body = self._read_json_body()
                autonomy = body.get(
                    "autonomy",
                    {
                        "default": "propose_wait",
                        "action_levels": {
                            "add_negative_keywords": "auto_execute",
                            "pause_campaign": "propose_wait",
                            "adjust_bid": "propose_wait",
                            "draft_campaign": "draft_review",
                        },
                        "escalation": {"spend_anomaly_pct": 50, "roas_drop_pct": 45},
                    },
                )
                account = self.context.db.create_account(
                    name=str(body["name"]),
                    customer_id=str(body["customer_id"]),
                    vertical=str(body.get("vertical", "general")),
                    timezone_value=str(body.get("timezone", self.context.settings.timezone)),
                    slack_channel=str(body.get("slack_channel", "")),
                    autonomy_json=json.dumps(autonomy),
                    data_source=str(body.get("data_source", "demo")),
                    google_ads_customer_name=body.get("google_ads_customer_name"),
                )
                self._send_json(HTTPStatus.CREATED, {"ok": True, "account": account})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        if path == "/api/google-ads/import-account":
            try:
                body = self._read_json_body()
                if not self.context.settings.has_google_ads_credentials:
                    raise ValueError("Google Ads credentials are not configured")

                customer_id = str(body["customer_id"]).strip()
                summary = self.context.ads.describe_customer(customer_id)
                account_name = str(body.get("name") or summary.get("descriptive_name") or f"Google Ads {customer_id}")
                autonomy = body.get(
                    "autonomy",
                    {
                        "default": "propose_wait",
                        "action_levels": {
                            "add_negative_keywords": "auto_execute",
                            "pause_campaign": "propose_wait",
                            "adjust_bid": "propose_wait",
                            "draft_campaign": "draft_review",
                        },
                        "escalation": {"spend_anomaly_pct": 50, "roas_drop_pct": 45},
                    },
                )
                account = self.context.db.create_account(
                    name=account_name,
                    customer_id=customer_id,
                    vertical=str(body.get("vertical", "general")),
                    timezone_value=str(body.get("timezone") or summary.get("time_zone") or self.context.settings.timezone),
                    slack_channel=str(body.get("slack_channel", "")),
                    autonomy_json=json.dumps(autonomy),
                    data_source="live",
                    google_ads_customer_name=str(summary.get("descriptive_name", "")),
                )
                self._send_json(
                    HTTPStatus.CREATED,
                    {
                        "ok": True,
                        "account": account,
                        "google_ads_customer": summary,
                    },
                )
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        if path == "/api/tools/run":
            try:
                body = self._read_json_body()
                result = self.context.tools.run_tool(
                    tool_name=str(body["tool_name"]),
                    account_id=body.get("account_id"),
                    params=body.get("params", {}),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        if path == "/api/run-monitoring":
            try:
                body = self._read_json_body()
                account_id = body.get("account_id")
                result = self.context.orchestrator.run_monitoring_cycle(
                    account_id=int(account_id) if account_id is not None else None,
                    triggered_by="manual_api",
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        m = re.fullmatch(r"/api/alerts/(\d+)/decision", path)
        if m:
            alert_id = int(m.group(1))
            try:
                body = self._read_json_body()
                result = self.context.orchestrator.apply_alert_decision(
                    alert_id=alert_id,
                    decision=str(body.get("decision", "")).strip().lower(),
                    actor=str(body.get("actor", "human")),
                    modifications=body.get("modifications", {}),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        if path == "/api/reports/weekly/generate":
            try:
                report = self.context.reports.generate_weekly_mcc_report()
                self._send_json(HTTPStatus.OK, {"ok": True, "report": report})
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return

        if path == "/api/reports/monthly/generate":
            try:
                body = self._read_json_body()
                aid = int(body["account_id"])
                report = self.context.reports.generate_monthly_client_report(aid)
                self._send_json(HTTPStatus.OK, {"ok": True, "report": report})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        m = re.fullmatch(r"/api/context/(\d+)", path)
        if m:
            aid = int(m.group(1))
            try:
                body = self._read_json_body()
                key = str(body["key"])
                value = str(body["value"])
                self.context.db.upsert_context_memory(aid, key, value)
                self._send_json(HTTPStatus.OK, {"ok": True})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        self._not_found()


def run_server() -> None:
    settings = load_settings()
    context = AppContext(settings)
    context.start_scheduler()

    RequestHandler.context = context
    server = ThreadingHTTPServer((settings.app_host, settings.app_port), RequestHandler)

    print(f"Ads Genie running at http://{settings.app_host}:{settings.app_port}")
    print(f"Mode: {context.ads.mode} | Scheduler: {'enabled' if settings.enable_scheduler else 'disabled'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        context.stop()


if __name__ == "__main__":
    run_server()
