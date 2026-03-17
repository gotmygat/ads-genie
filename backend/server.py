from __future__ import annotations

from base64 import b64decode
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import uuid4
import json
import re

from .actions import ActionExecutor
from .ads_client import GoogleAdsAdapter
from .config import Settings, load_settings
from .db import Database
from .observability import RuntimeMonitor, configure_logging
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
        self.monitor = RuntimeMonitor(self.db, service="ads-genie", environment=settings.environment)
        if settings.auto_seed:
            self.db.seed_demo_data()

        self.ads = GoogleAdsAdapter(settings, self.db, monitor=self.monitor)
        self.tools = ToolEngine(self.db, self.ads)
        self.actions = ActionExecutor(self.db, self.ads, self.monitor)
        self.reports = ReportService(self.db, self.tools, settings.timezone)
        self.slack = SlackBridge(settings, self.db, self.monitor)
        self.orchestrator = Orchestrator(
            self.db,
            self.tools,
            self.actions,
            self.reports,
            self.slack,
            self.monitor,
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
        print(f"[ads-genie] {self.address_string()} - {fmt % args}")

    @property
    def request_id(self) -> str:
        value = getattr(self, "_request_id", "")
        if not value:
            value = uuid4().hex[:12]
            self._request_id = value
        return value

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("X-Request-Id", self.request_id)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _send_text(self, status: int, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
        raw = text.encode("utf-8")
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Request-Id", self.request_id)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _read_body_bytes(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _read_json_body(self) -> dict[str, Any]:
        raw = self._read_body_bytes()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _not_found(self) -> None:
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not Found", "request_id": self.request_id})

    def _send_unauthorized(self) -> None:
        self._response_status = HTTPStatus.UNAUTHORIZED
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("WWW-Authenticate", 'Basic realm="Ads Genie"')
        self.send_header("X-Request-Id", self.request_id)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(json.dumps({"ok": False, "error": "Authentication required", "request_id": self.request_id}).encode("utf-8"))

    def _parse_basic_auth(self) -> tuple[str, str] | None:
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return None
        try:
            decoded = b64decode(header.split(" ", 1)[1].strip()).decode("utf-8")
        except Exception:
            return None
        if ":" not in decoded:
            return None
        username, password = decoded.split(":", 1)
        return username, password

    def _auth_required(self, path: str) -> bool:
        if not self.context.settings.auth_is_configured:
            return False
        if path in {"/api/health", "/api/slack/interactivity"}:
            return False
        return True

    def _check_auth(self, path: str) -> bool:
        if not self._auth_required(path):
            return True
        creds = self._parse_basic_auth()
        if not creds:
            self.context.monitor.emit("warning", "auth_failed", "Missing auth header", request_id=self.request_id, details={"path": path})
            self._send_unauthorized()
            return False
        username, password = creds
        if username != self.context.settings.app_auth_username or password != self.context.settings.app_auth_password:
            self.context.monitor.emit("warning", "auth_failed", "Invalid credentials", request_id=self.request_id, details={"path": path, "username": username})
            self._send_unauthorized()
            return False
        return True

    def _serve_frontend_file(self, filename: str, content_type: str) -> None:
        target = FRONTEND_DIR / filename
        if not target.exists():
            self._send_text(HTTPStatus.NOT_FOUND, "Missing frontend asset")
            return
        self._send_text(HTTPStatus.OK, target.read_text(encoding="utf-8"), content_type=content_type)

    def _runtime_summary(self) -> dict[str, Any]:
        events = self.context.db.list_runtime_events(limit=200)
        by_level: dict[str, int] = {}
        by_category: dict[str, int] = {}
        for event in events:
            by_level[str(event["level"])] = by_level.get(str(event["level"]), 0) + 1
            by_category[str(event["category"])] = by_category.get(str(event["category"]), 0) + 1
        return {
            "event_count": len(events),
            "by_level": by_level,
            "top_categories": sorted(by_category.items(), key=lambda item: item[1], reverse=True)[:8],
        }

    def _handle_slack_interactivity(self, raw_body: bytes) -> None:
        headers = {str(k).lower(): v for k, v in self.headers.items()}
        if not self.context.slack.verify_signature(headers, raw_body):
            self._send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "Invalid Slack signature", "request_id": self.request_id})
            return

        form = parse_qs(raw_body.decode("utf-8"))
        payload_raw = form.get("payload", ["{}"])[0]
        payload = json.loads(payload_raw)
        payload_type = str(payload.get("type", ""))

        if payload_type == "block_actions":
            action = (payload.get("actions") or [{}])[0]
            action_id = str(action.get("action_id", ""))
            value = json.loads(action.get("value", "{}") or "{}")
            alert_id = int(value["alert_id"])
            actor = f"slack_user:{payload.get('user', {}).get('id', 'unknown')}"

            if action_id == "ads_genie_modify":
                trigger_id = str(payload.get("trigger_id", ""))
                result = self.context.slack.open_modify_modal(trigger_id, alert_id)
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
                return

            decision = "approve" if action_id == "ads_genie_approve" else "dismiss"
            result = self.context.orchestrator.apply_alert_decision(alert_id=alert_id, decision=decision, actor=actor, modifications={})
            alert = self.context.db.get_alert(alert_id) or {}
            blocks = self.context.slack.build_resolution_blocks(
                title=str(alert.get("title", "Ads Genie Alert")),
                detail=f"{decision.title()}d from Slack",
                status="executed" if decision == "approve" else "dismissed",
            )
            self.context.slack.update_alert_message(
                alert_id=alert_id,
                text=f"{alert.get('title', 'Ads Genie Alert')} — {decision.title()}d",
                blocks=blocks,
            )
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if payload_type == "view_submission":
            view = payload.get("view", {})
            metadata = json.loads(view.get("private_metadata", "{}") or "{}")
            alert_id = int(metadata["alert_id"])
            actor = f"slack_user:{payload.get('user', {}).get('id', 'unknown')}"
            values = view.get("state", {}).get("values", {})
            instruction = ""
            for block in values.values():
                for item in block.values():
                    instruction = str(item.get("value", "")).strip()
                    if instruction:
                        break
                if instruction:
                    break
            result = self.context.orchestrator.apply_alert_decision(
                alert_id=alert_id,
                decision="modify",
                actor=actor,
                modifications={"note": instruction},
            )
            alert = self.context.db.get_alert(alert_id) or {}
            blocks = self.context.slack.build_resolution_blocks(
                title=str(alert.get("title", "Ads Genie Alert")),
                detail=f"Modified in Slack: {instruction or 'instruction recorded'}",
                status="executed",
            )
            self.context.slack.update_alert_message(
                alert_id=alert_id,
                text=f"{alert.get('title', 'Ads Genie Alert')} — Modified",
                blocks=blocks,
            )
            self._send_json(HTTPStatus.OK, {"response_action": "clear", "ok": True, "result": result})
            return

        self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": f"Unsupported Slack payload type: {payload_type}", "request_id": self.request_id})

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_HEAD(self) -> None:
        self._dispatch("HEAD")

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        self._request_id = uuid4().hex[:12]
        self._response_status = HTTPStatus.INTERNAL_SERVER_ERROR
        started = perf_counter()
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if not self._check_auth(path):
                return

            if method in {"GET", "HEAD"}:
                self._route_get(path, query)
            elif method == "POST":
                self._route_post(path)
            else:
                self._send_json(HTTPStatus.METHOD_NOT_ALLOWED, {"ok": False, "error": "Method Not Allowed", "request_id": self.request_id})
        except Exception as exc:
            self.context.monitor.exception("request_failed", exc, message="Unhandled request exception", request_id=self.request_id, details={"path": path, "method": method})
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc), "request_id": self.request_id})
        finally:
            duration_ms = round((perf_counter() - started) * 1000, 2)
            self.context.monitor.emit(
                "info",
                "http_request",
                f"{method} {path}",
                request_id=self.request_id,
                details={"path": path, "method": method, "status": int(self._response_status), "duration_ms": duration_ms},
            )

    def _route_get(self, path: str, query: dict[str, list[str]]) -> None:
        if path == "/api/health":
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "mode": self.context.ads.mode,
                    "environment": self.context.settings.environment,
                    "google_ads_configured": self.context.settings.has_google_ads_credentials,
                    "slack_configured": self.context.settings.has_slack_credentials,
                    "auth_configured": self.context.settings.auth_is_configured,
                    "scheduler_enabled": self.context.settings.enable_scheduler,
                    "monitor_interval_seconds": self.context.settings.monitor_interval_seconds,
                    "runtime": self._runtime_summary(),
                    "request_id": self.request_id,
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
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "request_id": self.request_id})
            return

        if path == "/api/google-ads/customers":
            if not self.context.settings.has_google_ads_credentials:
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "customers": [], "configured": False, "message": "Google Ads credentials not configured", "request_id": self.request_id},
                )
                return
            customers = self.context.ads.list_accessible_customers()
            self._send_json(HTTPStatus.OK, {"ok": True, "configured": True, "customers": customers, "request_id": self.request_id})
            return

        if path == "/api/tools":
            self._send_json(HTTPStatus.OK, {"ok": True, "tools": self.context.tools.list_tools(), "request_id": self.request_id})
            return

        if path == "/api/thresholds":
            self._send_json(HTTPStatus.OK, {"ok": True, "thresholds": self.context.tools.list_thresholds(), "request_id": self.request_id})
            return

        if path == "/api/system/events":
            level = query.get("level", [None])[0]
            category = query.get("category", [None])[0]
            events = self.context.db.list_runtime_events(level=level, category=category, limit=200)
            for item in events:
                item["details"] = json.loads(item.get("details_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "events": events, "request_id": self.request_id})
            return

        if path == "/api/tools/run-calibration":
            vertical = query.get("vertical", [None])[0]
            result = self.context.tools.calibrate_thresholds(vertical=vertical, apply=False)
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "request_id": self.request_id})
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
            self._send_json(HTTPStatus.OK, {"ok": True, "accounts": enriched, "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/accounts/(\d+)", path)
        if m:
            aid = int(m.group(1))
            account = self.context.db.get_account(aid)
            if not account:
                self._not_found()
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "account": account,
                    "campaigns": self.context.db.campaigns_for_account(aid),
                    "context_memory": self.context.db.list_context_memory(aid),
                    "negatives": self.context.db.list_negative_keywords(aid),
                    "request_id": self.request_id,
                },
            )
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/campaigns", path)
        if m:
            aid = int(m.group(1))
            self._send_json(HTTPStatus.OK, {"ok": True, "campaigns": self.context.db.campaigns_for_account(aid), "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/negatives", path)
        if m:
            aid = int(m.group(1))
            self._send_json(HTTPStatus.OK, {"ok": True, "negative_keywords": self.context.db.list_negative_keywords(aid), "request_id": self.request_id})
            return

        if path == "/api/alerts":
            status = query.get("status", [None])[0]
            alerts = self.context.db.list_alerts(status=status)
            for alert in alerts:
                alert["recommendation"] = json.loads(alert.get("recommendation_json", "{}"))
                alert["context"] = json.loads(alert.get("context_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "alerts": alerts, "request_id": self.request_id})
            return

        if path == "/api/decisions":
            decisions = self.context.db.list_decisions(limit=300)
            for decision in decisions:
                decision["payload"] = json.loads(decision.get("payload_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "decisions": decisions, "request_id": self.request_id})
            return

        if path == "/api/actions":
            actions = self.context.db.list_actions(limit=300)
            for action in actions:
                action["params"] = json.loads(action.get("params_json", "{}"))
            self._send_json(HTTPStatus.OK, {"ok": True, "actions": actions, "request_id": self.request_id})
            return

        if path == "/api/reports/weekly/latest":
            report = self.context.db.latest_report("weekly_mcc")
            self._send_json(HTTPStatus.OK, {"ok": True, "report": report, "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/reports/monthly/(\d+)/latest", path)
        if m:
            aid = int(m.group(1))
            report = self.context.db.latest_report("monthly_client", account_id=aid)
            self._send_json(HTTPStatus.OK, {"ok": True, "report": report, "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/context/(\d+)", path)
        if m:
            aid = int(m.group(1))
            memories = self.context.db.list_context_memory(aid)
            self._send_json(HTTPStatus.OK, {"ok": True, "context_memory": memories, "request_id": self.request_id})
            return

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

    def _route_post(self, path: str) -> None:
        if path == "/api/slack/interactivity":
            raw_body = self._read_body_bytes()
            self._handle_slack_interactivity(raw_body)
            return

        if path == "/api/accounts":
            try:
                body = self._read_json_body()
                autonomy = body.get(
                    "autonomy",
                    {
                        "default": "propose_wait",
                        "action_levels": {
                            "add_negative_keywords": "propose_wait",
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
                self._send_json(HTTPStatus.CREATED, {"ok": True, "account": account, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
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
                            "add_negative_keywords": "propose_wait",
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
                self._send_json(HTTPStatus.CREATED, {"ok": True, "account": account, "google_ads_customer": summary, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/tools/run":
            try:
                body = self._read_json_body()
                result = self.context.tools.run_tool(
                    tool_name=str(body["tool_name"]),
                    account_id=body.get("account_id"),
                    params=body.get("params", {}),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/run-monitoring":
            try:
                body = self._read_json_body()
                account_id = body.get("account_id")
                result = self.context.orchestrator.run_monitoring_cycle(
                    account_id=int(account_id) if account_id is not None else None,
                    triggered_by="manual_api",
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
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
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/reports/weekly/generate":
            try:
                report = self.context.reports.generate_weekly_mcc_report()
                self._send_json(HTTPStatus.OK, {"ok": True, "report": report, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/reports/monthly/generate":
            try:
                body = self._read_json_body()
                aid = int(body["account_id"])
                report = self.context.reports.generate_monthly_client_report(aid)
                self._send_json(HTTPStatus.OK, {"ok": True, "report": report, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/context/(\d+)", path)
        if m:
            aid = int(m.group(1))
            try:
                body = self._read_json_body()
                key = str(body["key"])
                value = str(body["value"])
                self.context.db.upsert_context_memory(aid, key, value)
                self._send_json(HTTPStatus.OK, {"ok": True, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/thresholds/override":
            try:
                body = self._read_json_body()
                self.context.db.upsert_threshold_override(
                    vertical=str(body["vertical"]),
                    roas_healthy=float(body["roas_healthy"]),
                    cpa_target=float(body["cpa_target"]),
                    quality_score_min=float(body["quality_score_min"]),
                    source=str(body.get("source", "manual_override")),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "thresholds": self.context.tools.list_thresholds(), "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/thresholds/calibrate":
            try:
                body = self._read_json_body()
                result = self.context.tools.calibrate_thresholds(
                    vertical=body.get("vertical"),
                    apply=bool(body.get("apply", False)),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "result": result, "thresholds": self.context.tools.list_thresholds(), "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        self._not_found()


def run_server() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
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
