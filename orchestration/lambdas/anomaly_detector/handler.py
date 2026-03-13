from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any
import json
import logging
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from orchestration.models.account_registry import AccountRegistry
from orchestration.models.autonomy_config import AutonomyConfig


LOGGER = logging.getLogger(__name__)


def _dedupe_key(detail: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "customer_id": detail.get("customer_id"),
            "overall_status": detail.get("health_check", {}).get("overall_status"),
            "roas_status": detail.get("health_check", {}).get("roas_status"),
            "quality_score_status": detail.get("health_check", {}).get("quality_score_status"),
            "zero_conversion_campaigns": detail.get("health_check", {}).get("zero_conversion_campaigns", []),
        },
        sort_keys=True,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _dedupe_hit(cache_table: Any, dedupe_key: str) -> bool:
    if cache_table is None:
        return False
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    item = cache_table.get_item(Key={"cache_key": dedupe_key}).get("Item")
    return bool(item and int(item.get("ttl_epoch", 0)) > now_epoch)


def _write_dedupe_marker(cache_table: Any, dedupe_key: str, customer_id: str) -> None:
    if cache_table is None:
        return
    ttl_epoch = int((datetime.now(timezone.utc) + timedelta(hours=4)).timestamp())
    cache_table.put_item(
        Item={
            "cache_key": dedupe_key,
            "customer_id": customer_id,
            "query_hash": "anomaly_dedupe",
            "payload": {"type": "anomaly_dedupe"},
            "ttl_epoch": ttl_epoch,
        }
    )


def _recommend_action_type(detail: dict[str, Any]) -> str:
    health = detail.get("health_check", {})
    if health.get("zero_conversion_campaigns"):
        return "add_negative_keywords"
    if str(health.get("quality_score_status", "")).lower() != "healthy":
        return "adjust_bids"
    return "adjust_bids"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    detail = event.get("detail", event)
    action_type = _recommend_action_type(detail)
    customer_id = str(detail.get("customer_id", ""))
    registry = AccountRegistry()
    account = registry.get_account(customer_id) or {}
    autonomy = AutonomyConfig()
    policy = autonomy.get_action_policy(str(account.get("autonomy_config_id", "")), action_type)
    autonomy_level = str(policy.get("level", "escalate"))

    cache_table = None
    if boto3 is not None and os.getenv("DYNAMODB_CACHE_TABLE", ""):
        cache_table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")).Table(
            os.getenv("DYNAMODB_CACHE_TABLE", "")
        )

    dedupe_key = _dedupe_key(detail)
    if _dedupe_hit(cache_table, dedupe_key):
        response = {"suppressed": True, "reason": "duplicate_within_4h", "customer_id": customer_id, "action_type": action_type}
        LOGGER.info("anomaly_suppressed", extra=response)
        return response
    _write_dedupe_marker(cache_table, dedupe_key, customer_id)

    execution_input = {
        "customer_id": customer_id,
        "account_name": detail.get("account_name"),
        "vertical": detail.get("vertical"),
        "slack_channel_id": detail.get("slack_channel_id"),
        "autonomy_config_id": detail.get("autonomy_config_id"),
        "health_check": detail.get("health_check", {}),
        "action_type": action_type,
        "autonomy_level": autonomy_level,
        "detected_at": detail.get("detected_at"),
    }

    execution_arn = None
    state_machine_arn = os.getenv("STEP_FUNCTIONS_ANOMALY_ARN", "")
    if boto3 is not None and state_machine_arn:
        stepfunctions = boto3.client("stepfunctions", region_name=os.getenv("AWS_REGION", "us-east-1"))
        start = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(execution_input),
        )
        execution_arn = start.get("executionArn")

    response = {
        "suppressed": False,
        "customer_id": customer_id,
        "action_type": action_type,
        "autonomy_level": autonomy_level,
        "execution_arn": execution_arn,
        "request_id": getattr(context, "aws_request_id", None),
    }
    LOGGER.info("anomaly_detector_started_workflow", extra=response)
    return response
