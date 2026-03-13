from __future__ import annotations

from typing import Any
import json
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

try:
    from slack_sdk import WebClient
except ImportError:  # pragma: no cover
    WebClient = None  # type: ignore[assignment]

from mcp_server.write_actions.add_negative_keywords import add_negative_keywords
from mcp_server.write_actions.adjust_bids import adjust_bids
from mcp_server.write_actions.enable_ad_group import enable_ad_group
from mcp_server.write_actions.pause_ad_group import pause_ad_group
from orchestration.models.account_registry import AccountRegistry
from orchestration.models.decision_log import DecisionLog
from slack_bot.messages.confirmation_blocks import build_confirmation_blocks


WRITE_ACTIONS = {
    "add_negative_keywords": add_negative_keywords,
    "pause_ad_group": pause_ad_group,
    "enable_ad_group": enable_ad_group,
    "adjust_bids": adjust_bids,
}


def _slack_token() -> str:
    if os.getenv("ENV", "local").strip().lower() == "production" and boto3 is not None:
        secret_name = os.getenv("SLACK_SECRET_NAME", "ads-genie/slack")
        client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
        payload = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
        return str(payload.get("SLACK_BOT_TOKEN", ""))
    return os.getenv("SLACK_BOT_TOKEN", "")


def _post_slack_confirmation(channel: str, blocks: list[dict[str, Any]]) -> None:
    token = _slack_token().strip()
    if not token or WebClient is None or not channel:
        return
    WebClient(token=token).chat_postMessage(channel=channel, text="Action executed", blocks=blocks)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    action_type = str(event["action_type"])
    decision = str(event.get("decision", "approved"))
    if decision == "dismissed":
        DecisionLog().write(
            {
                "customer_id": event["customer_id"],
                "action_type": action_type,
                "human_decision": "dismissed",
                "recommended_action": event,
                "tool_call_inputs": event,
                "tool_call_outputs": {"status": "dismissed"},
                "step_functions_execution_id": event.get("execution_arn"),
            }
        )
        return {"status": "dismissed", "customer_id": event["customer_id"], "action_type": action_type}

    payload = dict(event.get("modified_action") or event.get("action_payload") or event.get("recommended_action_payload") or {})
    action_fn = WRITE_ACTIONS[action_type]
    registry = AccountRegistry()
    account = registry.get_account(str(event["customer_id"])) or {}

    if action_type == "add_negative_keywords":
        result = action_fn(
            customer_id=str(event["customer_id"]),
            campaign_id=str(payload["campaign_id"]),
            negative_keywords=list(payload["negative_keywords"]),
            scope=str(payload.get("scope", "campaign")),
            ad_group_id=payload.get("ad_group_id"),
            step_functions_execution_id=event.get("execution_arn") or getattr(context, "aws_request_id", None),
        )
    elif action_type == "pause_ad_group":
        result = action_fn(
            customer_id=str(event["customer_id"]),
            ad_group_id=str(payload["ad_group_id"]),
            step_functions_execution_id=event.get("execution_arn") or getattr(context, "aws_request_id", None),
        )
    elif action_type == "enable_ad_group":
        result = action_fn(
            customer_id=str(event["customer_id"]),
            ad_group_id=str(payload["ad_group_id"]),
            step_functions_execution_id=event.get("execution_arn") or getattr(context, "aws_request_id", None),
        )
    else:
        result = action_fn(
            customer_id=str(event["customer_id"]),
            ad_group_id=str(payload["ad_group_id"]),
            requested_change_pct=float(payload["requested_change_pct"]),
            current_cpc_bid_micros=int(payload["current_cpc_bid_micros"]),
            step_functions_execution_id=event.get("execution_arn") or getattr(context, "aws_request_id", None),
        )

    blocks = build_confirmation_blocks(
        action_type=action_type,
        target_label=result.target_resource or action_type,
        google_ads_operation_ids=result.google_ads_operation_ids,
        decision_log_url=None,
        timestamp=result.timestamp.isoformat(),
    )
    _post_slack_confirmation(str(account.get("slack_channel_id", event.get("slack_channel_id", ""))), blocks)
    return {
        "status": "executed",
        "customer_id": event["customer_id"],
        "action_type": action_type,
        "result": result.model_dump(mode="json"),
    }
