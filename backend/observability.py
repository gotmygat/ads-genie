from __future__ import annotations

from typing import Any
import json
import logging
import sys
import traceback

from .db import Database

SENSITIVE_KEYS = {
    "authorization",
    "password",
    "pass",
    "secret",
    "token",
    "api_key",
    "apikey",
    "client_secret",
    "refresh_token",
}


def _redact(value: Any, *, key_hint: str = "") -> Any:
    key_norm = key_hint.strip().lower()
    if any(marker in key_norm for marker in SENSITIVE_KEYS):
        return "***REDACTED***"
    if isinstance(value, dict):
        return {str(k): _redact(v, key_hint=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item, key_hint=key_hint) for item in value]
    return value


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, (level or "INFO").upper(), logging.INFO),
        stream=sys.stdout,
        format="%(message)s",
        force=True,
    )


class RuntimeMonitor:
    def __init__(
        self,
        db: Database,
        service: str = "ads-genie",
        environment: str = "local",
        include_tracebacks: bool = False,
    ) -> None:
        self.db = db
        self.service = service
        self.environment = environment
        self.include_tracebacks = include_tracebacks
        self.logger = logging.getLogger(service)

    def emit(
        self,
        level: str,
        category: str,
        message: str,
        *,
        account_id: int | None = None,
        alert_id: int | None = None,
        action_id: int | None = None,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> int:
        safe_details = _redact(details or {})
        payload = {
            "service": self.service,
            "environment": self.environment,
            "level": level.upper(),
            "category": category,
            "message": message,
            "account_id": account_id,
            "alert_id": alert_id,
            "action_id": action_id,
            "request_id": request_id,
            "details": safe_details,
        }
        self.logger.log(getattr(logging, payload["level"], logging.INFO), json.dumps(payload, ensure_ascii=True))
        return self.db.insert_runtime_event(
            level=payload["level"].lower(),
            category=category,
            message=message,
            details=safe_details,
            account_id=account_id,
            alert_id=alert_id,
            action_id=action_id,
            request_id=request_id,
        )

    def exception(
        self,
        category: str,
        exc: BaseException,
        *,
        message: str | None = None,
        account_id: int | None = None,
        alert_id: int | None = None,
        action_id: int | None = None,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> int:
        payload = dict(details or {})
        payload["error"] = str(exc)
        if self.include_tracebacks:
            payload["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-8000:]
        return self.emit(
            "error",
            category,
            message or str(exc),
            account_id=account_id,
            alert_id=alert_id,
            action_id=action_id,
            request_id=request_id,
            details=payload,
        )
