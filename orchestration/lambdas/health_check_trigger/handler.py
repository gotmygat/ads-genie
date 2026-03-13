from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from time import perf_counter
from typing import Any
import json
import logging
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.gaql.queries import QueryExecutor
from mcp_server.tools.health_check import health_check
from orchestration.models.account_registry import AccountRegistry


LOGGER = logging.getLogger(__name__)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    started = perf_counter()
    registry = AccountRegistry()
    auth = GoogleAdsAuth()
    query_executor = QueryExecutor()
    accounts = registry.list_active_accounts()
    accounts_by_vertical: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for account in accounts:
        accounts_by_vertical[str(account.get("vertical", "unknown"))].append(account)

    bus_name = os.getenv("EVENTBRIDGE_BUS_NAME", "default")
    events_client = boto3.client("events", region_name=os.getenv("AWS_REGION", "us-east-1")) if boto3 is not None else None
    entries: list[dict[str, Any]] = []
    unhealthy_accounts: list[str] = []

    for account in accounts:
        result = health_check(
            customer_id=str(account["customer_id"]),
            vertical=str(account["vertical"]),
            auth=auth,
            query_executor=query_executor,
        )
        if result.overall_status == "healthy":
            continue
        unhealthy_accounts.append(str(account["customer_id"]))
        detail = {
            "customer_id": str(account["customer_id"]),
            "account_name": str(account.get("account_name", account["customer_id"])),
            "vertical": str(account["vertical"]),
            "slack_channel_id": str(account.get("slack_channel_id", "")),
            "autonomy_config_id": str(account.get("autonomy_config_id", "")),
            "health_check": result.model_dump(mode="json"),
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        entries.append(
            {
                "Source": "ads_genie.health_check",
                "DetailType": "account_health_alert",
                "Detail": json.dumps(detail),
                "EventBusName": bus_name,
            }
        )

    if entries and events_client is not None:
        for offset in range(0, len(entries), 10):
            events_client.put_events(Entries=entries[offset : offset + 10])

    duration_ms = round((perf_counter() - started) * 1000, 2)
    response = {
        "account_count": len(accounts),
        "vertical_groups": {key: len(value) for key, value in accounts_by_vertical.items()},
        "anomaly_count": len(unhealthy_accounts),
        "unhealthy_accounts": unhealthy_accounts,
        "duration_ms": duration_ms,
        "request_id": getattr(context, "aws_request_id", None),
        "event": event,
    }
    LOGGER.info("health_check_trigger_complete", extra=response)
    return response
