from __future__ import annotations

from base64 import b64decode
from hmac import compare_digest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import uuid4
import json
import re
import xml.etree.ElementTree as ET
import zipfile

from .actions import ActionExecutor
from .ads_client import GoogleAdsAdapter
from .config import Settings, load_settings
from .db import Database
from .observability import RuntimeMonitor, configure_logging
from .orchestrator import Orchestrator, SchedulerThread
from .reports import ReportService
from .slack_bridge import SlackBridge
from .tools import ToolEngine
from orchestration.models.autonomy_levels import normalize_autonomy_level


ROOT_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = ROOT_DIR / "frontend"

DEFAULT_AUTONOMY_POLICY = {
    "default": "propose_and_wait",
    "action_levels": {
        "add_negative_keywords": "propose_and_wait",
        "pause_campaign": "propose_and_wait",
        "adjust_bid": "propose_and_wait",
        "draft_campaign": "draft_and_review",
    },
    "escalation": {"spend_anomaly_pct": 50, "roas_drop_pct": 45},
}
ALLOWED_AUTONOMY_LEVELS = {"propose_and_wait", "draft_and_review", "escalate"}
SUPPORTED_CONTEXT_EXTENSIONS = {".txt", ".md", ".csv", ".json", ".yaml", ".yml", ".xml", ".html", ".htm", ".docx"}
MAX_CONTEXT_FILES = 6
MAX_CONTEXT_FILE_BYTES = 1_500_000
MAX_CONTEXT_TEXT_CHARS = 16_000
MAX_TOTAL_CONTEXT_CHARS = 48_000


def _coerce_float(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, numeric))


def _sanitize_autonomy(raw: Any) -> dict[str, Any]:
    policy = json.loads(json.dumps(DEFAULT_AUTONOMY_POLICY))
    if not isinstance(raw, dict):
        return policy

    candidate_default = normalize_autonomy_level(raw.get("default", policy["default"]), default=policy["default"])
    if candidate_default in ALLOWED_AUTONOMY_LEVELS:
        policy["default"] = candidate_default

    candidate_levels = raw.get("action_levels", {})
    if isinstance(candidate_levels, dict):
        for action_type in policy["action_levels"]:
            candidate = normalize_autonomy_level(
                candidate_levels.get(action_type, policy["action_levels"][action_type]),
                default=policy["action_levels"][action_type],
            )
            if candidate in ALLOWED_AUTONOMY_LEVELS:
                policy["action_levels"][action_type] = candidate

    escalation = raw.get("escalation", {})
    if isinstance(escalation, dict):
        policy["escalation"]["spend_anomaly_pct"] = _coerce_float(
            escalation.get("spend_anomaly_pct"),
            policy["escalation"]["spend_anomaly_pct"],
            minimum=10.0,
            maximum=200.0,
        )
        policy["escalation"]["roas_drop_pct"] = _coerce_float(
            escalation.get("roas_drop_pct"),
            policy["escalation"]["roas_drop_pct"],
            minimum=10.0,
            maximum=200.0,
        )
    return policy


def _parse_quiet_hour(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        hour = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Quiet hours must be integers between 0 and 23") from exc
    if hour < 0 or hour > 23:
        raise ValueError("Quiet hours must be integers between 0 and 23")
    return hour


def _coerce_monthly_budget(value: Any, default: float = 3000.0) -> float:
    try:
        budget = float(value)
    except (TypeError, ValueError):
        return default
    return max(500.0, min(250000.0, budget))


def _decode_text_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _extract_docx_text(raw: bytes) -> str:
    try:
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            document_xml = archive.read("word/document.xml")
    except Exception as exc:
        raise ValueError("Unable to read DOCX file") from exc
    root = ET.fromstring(document_xml)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    parts = [node.text.strip() for node in root.findall(".//w:t", namespace) if node.text and node.text.strip()]
    return "\n".join(parts)


def _extract_uploaded_text(filename: str, content_type: str, raw: bytes) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".docx":
        return _extract_docx_text(raw)
    if (
        suffix in SUPPORTED_CONTEXT_EXTENSIONS
        or str(content_type).startswith("text/")
        or content_type in {"application/json", "application/xml"}
    ):
        return _decode_text_bytes(raw)
    raise ValueError("Unsupported file type. Supported uploads: txt, md, csv, json, html, xml, yaml, docx.")


def _normalize_uploaded_files(raw_files: Any) -> list[dict[str, Any]]:
    if raw_files is None or raw_files == "":
        return []
    if not isinstance(raw_files, list):
        raise ValueError("files must be an array")
    if len(raw_files) > MAX_CONTEXT_FILES:
        raise ValueError(f"Upload up to {MAX_CONTEXT_FILES} files per campaign draft")

    normalized: list[dict[str, Any]] = []
    total_chars = 0
    for index, item in enumerate(raw_files, start=1):
        if not isinstance(item, dict):
            raise ValueError("Each file entry must be an object")
        filename = Path(str(item.get("name") or f"context-{index}.txt")).name[:120]
        content_type = str(item.get("type") or "")
        content_b64 = str(item.get("content_base64") or "")
        if not content_b64:
            raise ValueError(f"{filename} is missing content")
        try:
            raw = b64decode(content_b64.encode("utf-8"))
        except Exception as exc:
            raise ValueError(f"{filename} could not be decoded") from exc
        if len(raw) > MAX_CONTEXT_FILE_BYTES:
            raise ValueError(f"{filename} exceeds the {MAX_CONTEXT_FILE_BYTES // 1_000_000}MB file limit")
        text = re.sub(r"\n{3,}", "\n\n", _extract_uploaded_text(filename, content_type, raw)).strip()
        if not text:
            raise ValueError(f"{filename} did not contain readable text")
        trimmed = text[:MAX_CONTEXT_TEXT_CHARS]
        total_chars += len(trimmed)
        if total_chars > MAX_TOTAL_CONTEXT_CHARS:
            raise ValueError("Uploaded context exceeds the combined text limit")
        normalized.append(
            {
                "filename": filename,
                "content_type": content_type or "application/octet-stream",
                "size_bytes": len(raw),
                "content_text": trimmed,
                "excerpt": trimmed[:240],
            }
        )
    return normalized


class AppContext:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.db.init_schema()
        self.monitor = RuntimeMonitor(self.db, service="ads-genie", environment=settings.environment, include_tracebacks=False)
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

    def _request_origin(self) -> str:
        return str(self.headers.get("Origin", "")).strip()

    def _is_allowed_origin(self, origin: str) -> bool:
        return bool(origin) and origin in self.context.settings.app_allowed_origins

    def _apply_cors(self) -> None:
        origin = self._request_origin()
        if self._is_allowed_origin(origin):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self._apply_cors()
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
        if content_type.startswith("text/html"):
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; base-uri 'none'; "
                "frame-ancestors 'none'; object-src 'none'",
            )
        self._apply_cors()
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
        self._apply_cors()
        self.send_header("X-Request-Id", self.request_id)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(json.dumps({"ok": False, "error": "Authentication required", "request_id": self.request_id}).encode("utf-8"))

    def _send_auth_not_configured(self) -> None:
        self._send_json(
            HTTPStatus.SERVICE_UNAVAILABLE,
            {
                "ok": False,
                "error": "Authentication is required but APP_AUTH_USERNAME/APP_AUTH_PASSWORD are not configured.",
                "request_id": self.request_id,
            },
        )

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
        if path in {"/api/health", "/api/slack/interactivity"}:
            return False
        if path == "/api/system/events":
            return True
        return self.context.settings.auth_is_required

    def _check_auth(self, path: str) -> bool:
        if not self._auth_required(path):
            return True
        if not self.context.settings.auth_is_configured:
            self.context.monitor.emit(
                "warning",
                "auth_misconfigured",
                "Authentication required but credentials are not configured",
                request_id=self.request_id,
                details={"path": path},
            )
            self._send_auth_not_configured()
            return False
        creds = self._parse_basic_auth()
        if not creds:
            self.context.monitor.emit("warning", "auth_failed", "Missing auth header", request_id=self.request_id, details={"path": path})
            self._send_unauthorized()
            return False
        username, password = creds
        if not compare_digest(username, self.context.settings.app_auth_username) or not compare_digest(password, self.context.settings.app_auth_password):
            self.context.monitor.emit("warning", "auth_failed", "Invalid credentials", request_id=self.request_id, details={"path": path, "username": username})
            self._send_unauthorized()
            return False
        return True

    def _enforce_mutation_origin(self, path: str) -> bool:
        if path == "/api/slack/interactivity":
            return True
        origin = self._request_origin()
        sec_fetch_site = str(self.headers.get("Sec-Fetch-Site", "")).strip().lower()
        if origin and not self._is_allowed_origin(origin):
            self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "Origin not allowed", "request_id": self.request_id})
            return False
        if not origin and sec_fetch_site == "cross-site":
            self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "Cross-site request blocked", "request_id": self.request_id})
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

    def _serialize_campaign_draft(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        files = self.context.db.list_campaign_draft_files(int(row["id"]))
        return {
            "id": row["id"],
            "account_id": row["account_id"],
            "status": row["status"],
            "prompt_text": row["prompt_text"],
            "campaign_goal": row["campaign_goal"],
            "target_geography": row["target_geography"],
            "monthly_budget": row["monthly_budget"],
            "context_summary": row["context_summary"],
            "review_note": row.get("review_note"),
            "draft": json.loads(row.get("draft_json", "{}")),
            "files": [
                {
                    "id": item["id"],
                    "filename": item["filename"],
                    "content_type": item["content_type"],
                    "size_bytes": item["size_bytes"],
                    "created_at": item["created_at"],
                    "excerpt": str(item.get("content_text", ""))[:240],
                    "char_count": len(str(item.get("content_text", ""))),
                }
                for item in files
            ],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "approved_at": row.get("approved_at"),
        }

    def _build_draft_context_summary(
        self,
        account_id: int,
        prompt_text: str,
        files: list[dict[str, Any]],
        review_note: str = "",
    ) -> tuple[str, list[dict[str, Any]]]:
        memory_rows = self.context.db.list_context_memory(account_id)[:4]
        memory_briefs = [
            {
                "memory_key": row["memory_key"],
                "memory_value": str(row["memory_value"])[:180],
            }
            for row in memory_rows
        ]
        segments: list[str] = []
        if prompt_text:
            segments.append(f"Operator brief: {prompt_text[:280]}")
        if review_note:
            segments.append(f"Revision request: {review_note[:220]}")
        if files:
            file_labels = ", ".join(item["filename"] for item in files[:4])
            segments.append(f"Uploaded references: {file_labels}")
        if memory_briefs:
            segments.append(
                "Account context: "
                + " | ".join(f"{item['memory_key']}: {item['memory_value']}" for item in memory_briefs[:3])
            )
        return " ".join(segments).strip(), memory_briefs

    def _generate_campaign_draft(
        self,
        *,
        account_id: int,
        prompt_text: str,
        campaign_goal: str,
        target_geography: str,
        monthly_budget: float,
        files: list[dict[str, Any]],
        review_note: str = "",
        existing_draft_id: int | None = None,
    ) -> dict[str, Any]:
        account = self.context.db.get_account(account_id)
        if not account:
            raise ValueError("Unknown account")

        context_summary, memory_briefs = self._build_draft_context_summary(account_id, prompt_text, files, review_note)
        context_files = [
            {
                "filename": item["filename"],
                "excerpt": item["excerpt"],
                "char_count": len(item["content_text"]),
            }
            for item in files
        ]
        draft = self.context.tools.run_tool(
            "draft_campaign",
            account_id=account_id,
            params={
                "monthly_budget": monthly_budget,
                "campaign_goal": campaign_goal,
                "target_geography": target_geography,
                "prompt_text": prompt_text,
                "context_files": context_files,
            },
        )
        draft["context_summary"] = context_summary
        draft["source_context"] = {
            "prompt_text": prompt_text,
            "review_note": review_note,
            "files": context_files,
            "memory": memory_briefs,
        }
        draft["structure_explanation"] = {
            "title": "Why this draft is structured in STAG format",
            "bullets": [
                "Ad groups are separated by tight intent clusters to keep search terms and ad copy aligned.",
                "Shared negatives are carried across the build so expansion does not reintroduce known waste.",
                "Budget, CPA targets, and geo settings are inherited from the selected account and current benchmark posture.",
            ],
            "operator_prompt_used": bool(prompt_text),
        }
        draft["status_message"] = "Nothing executes until you approve"

        if existing_draft_id is None:
            stored = self.context.db.create_campaign_draft(
                account_id=account_id,
                prompt_text=prompt_text,
                campaign_goal=campaign_goal,
                target_geography=target_geography,
                monthly_budget=monthly_budget,
                context_summary=context_summary,
                draft=draft,
                files=files,
            )
            category = "campaign_draft_created"
            message = f"Campaign draft created for {account['name']}"
        else:
            stored = self.context.db.update_campaign_draft(
                existing_draft_id,
                prompt_text=prompt_text,
                campaign_goal=campaign_goal,
                target_geography=target_geography,
                monthly_budget=monthly_budget,
                context_summary=context_summary,
                review_note=review_note or None,
                draft=draft,
                files=files,
                status="draft",
            )
            category = "campaign_draft_updated"
            message = f"Campaign draft updated for {account['name']}"

        self.context.monitor.emit(
            "info",
            category,
            message,
            account_id=account_id,
            request_id=self.request_id,
            details={
                "draft_id": stored["id"] if stored else None,
                "campaign_goal": campaign_goal,
                "target_geography": target_geography,
                "file_count": len(files),
            },
        )
        return self._serialize_campaign_draft(stored) or {}

    def _notification_items(self, limit: int = 20) -> list[dict[str, Any]]:
        alerts = self.context.db.list_alerts(limit=max(8, limit))
        drafts = []
        for account in self.context.db.list_accounts():
            latest = self.context.db.latest_campaign_draft(int(account["id"]))
            if latest:
                drafts.append(latest)
        events = self.context.db.list_runtime_events(limit=max(12, limit))

        items: list[dict[str, Any]] = []
        for event in events:
            details = json.loads(event.get("details_json", "{}"))
            items.append(
                {
                    "id": f"event-{event['id']}",
                    "kind": "system",
                    "severity": event["level"],
                    "title": event["message"],
                    "body": details.get("path") or details.get("campaign_goal") or event["category"],
                    "account_id": event.get("account_id"),
                    "created_at": event["created_at"],
                }
            )
        for alert in alerts[:limit]:
            items.append(
                {
                    "id": f"alert-{alert['id']}",
                    "kind": "alert",
                    "severity": alert["severity"],
                    "title": alert["title"],
                    "body": alert["summary"],
                    "account_id": alert["account_id"],
                    "alert_id": alert["id"],
                    "created_at": alert["created_at"],
                }
            )
        for draft in drafts:
            parsed = json.loads(draft.get("draft_json", "{}"))
            items.append(
                {
                    "id": f"draft-{draft['id']}",
                    "kind": "draft",
                    "severity": "info",
                    "title": parsed.get("campaign_name") or "Campaign draft ready",
                    "body": draft.get("context_summary") or parsed.get("campaign_goal") or "Review pending",
                    "account_id": draft["account_id"],
                    "draft_id": draft["id"],
                    "created_at": draft["updated_at"],
                }
            )
        items.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        return items[:limit]

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
        origin = self._request_origin()
        if origin and not self._is_allowed_origin(origin):
            self.send_response(HTTPStatus.FORBIDDEN)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self._apply_cors()
        self.send_header("Access-Control-Max-Age", "600")
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
            if method == "POST" and not self._enforce_mutation_origin(path):
                return
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
                    "auth_required": self.context.settings.auth_is_required,
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

        if path == "/api/notifications":
            limit = max(1, min(50, int(query.get("limit", ["20"])[0] or 20)))
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "notifications": self._notification_items(limit=limit), "request_id": self.request_id},
            )
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

        m = re.fullmatch(r"/api/accounts/(\d+)/metrics", path)
        if m:
            aid = int(m.group(1))
            account = self.context.db.get_account(aid)
            if not account:
                self._not_found()
                return
            health = self.context.tools.health_check(aid, {})
            waste = self.context.tools.analyze_budget_waste(aid, {})
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "metrics": health.get("metrics", {}), "health": health, "waste": waste, "request_id": self.request_id},
            )
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/campaigns", path)
        if m:
            aid = int(m.group(1))
            self._send_json(HTTPStatus.OK, {"ok": True, "campaigns": self.context.db.campaigns_for_account(aid), "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/accounts/(\d+)/campaign-drafts", path)
        if m:
            aid = int(m.group(1))
            drafts = [self._serialize_campaign_draft(item) for item in self.context.db.list_campaign_drafts(aid, limit=25)]
            self._send_json(HTTPStatus.OK, {"ok": True, "drafts": [item for item in drafts if item], "request_id": self.request_id})
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

        if path == "/api/campaigns/draft/latest":
            account_id = int(query.get("account_id", ["0"])[0] or 0)
            if account_id <= 0:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "account_id is required", "request_id": self.request_id})
                return
            draft = self._serialize_campaign_draft(self.context.db.latest_campaign_draft(account_id))
            self._send_json(HTTPStatus.OK, {"ok": True, "draft": draft, "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/campaigns/draft/(\d+)", path)
        if m:
            draft_id = int(m.group(1))
            draft = self._serialize_campaign_draft(self.context.db.get_campaign_draft(draft_id))
            if not draft:
                self._not_found()
                return
            self._send_json(HTTPStatus.OK, {"ok": True, "draft": draft, "request_id": self.request_id})
            return

        if path in {"/", "/index.html", "/alerts", "/campaigns/draft"} or re.fullmatch(r"/campaigns/draft/\d+", path):
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
                autonomy = _sanitize_autonomy(body.get("autonomy"))
                account = self.context.db.create_account(
                    name=str(body["name"]),
                    customer_id=str(body["customer_id"]),
                    vertical=str(body.get("vertical", "general")),
                    timezone_value=str(body.get("timezone", self.context.settings.timezone)),
                    slack_channel=str(body.get("slack_channel", "")),
                    autonomy_json=json.dumps(autonomy),
                    data_source="demo",
                    google_ads_customer_name=body.get("google_ads_customer_name"),
                    quiet_hours_start=_parse_quiet_hour(body.get("quiet_hours_start")),
                    quiet_hours_end=_parse_quiet_hour(body.get("quiet_hours_end")),
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
                autonomy = _sanitize_autonomy(body.get("autonomy"))
                account = self.context.db.create_account(
                    name=account_name,
                    customer_id=customer_id,
                    vertical=str(body.get("vertical", "general")),
                    timezone_value=str(body.get("timezone") or summary.get("time_zone") or self.context.settings.timezone),
                    slack_channel=str(body.get("slack_channel", "")),
                    autonomy_json=json.dumps(autonomy),
                    data_source="live",
                    google_ads_customer_name=str(summary.get("descriptive_name", "")),
                    quiet_hours_start=_parse_quiet_hour(body.get("quiet_hours_start")),
                    quiet_hours_end=_parse_quiet_hour(body.get("quiet_hours_end")),
                )
                self._send_json(HTTPStatus.CREATED, {"ok": True, "account": account, "google_ads_customer": summary, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        if path == "/api/campaigns/draft":
            try:
                body = self._read_json_body()
                account_id = int(body["account_id"])
                account = self.context.db.get_account(account_id)
                if not account:
                    raise ValueError("Unknown account")
                prompt_text = str(body.get("prompt", "")).strip()
                campaign_goal = str(body.get("campaign_goal") or "Lead generation").strip() or "Lead generation"
                target_geography = str(body.get("target_geography") or f"{account['name']} +25mi").strip() or f"{account['name']} +25mi"
                monthly_budget = _coerce_monthly_budget(
                    body.get("monthly_budget"),
                    default=max(2500.0, round(float(self.context.tools.health_check(account_id, {}).get("metrics", {}).get("spend_7d", 0) or 0) * 4.2, 2)),
                )
                files = _normalize_uploaded_files(body.get("files", []))
                draft = self._generate_campaign_draft(
                    account_id=account_id,
                    prompt_text=prompt_text,
                    campaign_goal=campaign_goal,
                    target_geography=target_geography,
                    monthly_budget=monthly_budget,
                    files=files,
                )
                self._send_json(HTTPStatus.CREATED, {"ok": True, "draft": draft, "request_id": self.request_id})
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/campaigns/draft/(\d+)/approve", path)
        if m:
            draft_id = int(m.group(1))
            try:
                body = self._read_json_body()
                actor = str(body.get("actor", "dashboard_user"))
                draft_row = self.context.db.get_campaign_draft(draft_id)
                if not draft_row:
                    self._not_found()
                    return
                updated = self.context.db.mark_campaign_draft_status(draft_id, "approved")
                execution_arn = f"arn:aws:states:local:000000000000:execution:ads-genie:campaign-draft-{draft_id}-{uuid4().hex[:10]}"
                self.context.db.insert_decision(
                    account_id=int(draft_row["account_id"]),
                    actor=actor,
                    action="approve_campaign_draft",
                    payload={"draft_id": draft_id, "execution_arn": execution_arn},
                )
                self.context.monitor.emit(
                    "info",
                    "campaign_draft_approved",
                    f"Campaign draft approved for account {draft_row['account_id']}",
                    account_id=int(draft_row["account_id"]),
                    request_id=self.request_id,
                    details={"draft_id": draft_id, "execution_arn": execution_arn},
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "execution_arn": execution_arn,
                        "draft": self._serialize_campaign_draft(updated),
                        "request_id": self.request_id,
                    },
                )
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc), "request_id": self.request_id})
            return

        m = re.fullmatch(r"/api/campaigns/draft/(\d+)/modify", path)
        if m:
            draft_id = int(m.group(1))
            try:
                body = self._read_json_body()
                draft_row = self.context.db.get_campaign_draft(draft_id)
                if not draft_row:
                    self._not_found()
                    return
                review_note = str(body.get("note", "")).strip()
                if not review_note:
                    raise ValueError("A revision note is required")
                existing_files = self.context.db.list_campaign_draft_files(draft_id)
                files = _normalize_uploaded_files(body["files"]) if "files" in body else [
                    {
                        "filename": str(item["filename"]),
                        "content_type": str(item.get("content_type", "")),
                        "size_bytes": int(item.get("size_bytes", 0)),
                        "content_text": str(item.get("content_text", "")),
                        "excerpt": str(item.get("content_text", ""))[:240],
                    }
                    for item in existing_files
                ]
                updated = self._generate_campaign_draft(
                    account_id=int(draft_row["account_id"]),
                    prompt_text=str(body.get("prompt") or draft_row.get("prompt_text") or "").strip(),
                    campaign_goal=str(body.get("campaign_goal") or draft_row.get("campaign_goal") or "Lead generation").strip() or "Lead generation",
                    target_geography=str(body.get("target_geography") or draft_row.get("target_geography") or "Local radius +25mi").strip() or "Local radius +25mi",
                    monthly_budget=_coerce_monthly_budget(body.get("monthly_budget"), default=float(draft_row.get("monthly_budget") or 3000.0)),
                    files=files,
                    review_note=review_note,
                    existing_draft_id=draft_id,
                )
                self.context.db.insert_decision(
                    account_id=int(draft_row["account_id"]),
                    actor=str(body.get("actor", "dashboard_user")),
                    action="modify_campaign_draft",
                    payload={"draft_id": draft_id, "note": review_note},
                )
                self._send_json(HTTPStatus.OK, {"ok": True, "draft": updated, "request_id": self.request_id})
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
