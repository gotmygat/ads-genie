from __future__ import annotations

from typing import Any
import json
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None  # type: ignore[assignment]

from orchestration.models.decision_log import DecisionLog
from slack_bot.task_token_bridge.token_store import TokenStore


MODIFY_MODAL_CALLBACK_ID = "modify_action_submit"


def _stepfunctions_client(region_name: str | None = None):
    if boto3 is None:
        raise RuntimeError("boto3 is required for Step Functions callbacks")
    return boto3.client("stepfunctions", region_name=region_name or os.getenv("AWS_REGION", "us-east-1"))


def _message_ts_from_body(body: dict[str, Any]) -> str:
    return str(body.get("container", {}).get("message_ts") or body.get("message", {}).get("ts") or "")


def _decision_log_record(customer_id: str, action_type: str, human_decision: str, body: dict[str, Any], modified_action: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "customer_id": customer_id,
        "action_type": action_type,
        "recommended_action": json.loads(body.get("actions", [{}])[0].get("value", "{}") or "{}"),
        "human_decision": human_decision,
        "modified_action": modified_action,
        "slack_message_ts": _message_ts_from_body(body),
        "tool_call_inputs": body,
        "tool_call_outputs": {"decision": human_decision, "modified_action": modified_action},
    }


def approve_action(
    *,
    body: dict[str, Any],
    client: Any,
    token_store: TokenStore | None = None,
    decision_log: DecisionLog | None = None,
    stepfunctions_client: Any | None = None,
) -> None:
    token_store = token_store or TokenStore()
    decision_log = decision_log or DecisionLog()
    stepfunctions_client = stepfunctions_client or _stepfunctions_client()
    message_ts = _message_ts_from_body(body)
    token_record = token_store.retrieve_token(message_ts)
    if token_record is None:
        raise KeyError(f"No task token found for Slack message {message_ts}")
    stepfunctions_client.send_task_success(
        taskToken=token_record.task_token,
        output=json.dumps({"decision": "approved"}),
    )
    client.chat_update(
        channel=body["channel"]["id"],
        ts=message_ts,
        text="Action approved",
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Approved. Step Functions execution resumed."},
            }
        ],
    )
    decision_log.write(_decision_log_record(token_record.customer_id, token_record.action_type, "approved", body))


def open_modify_modal(*, body: dict[str, Any], client: Any) -> None:
    message_ts = _message_ts_from_body(body)
    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": MODIFY_MODAL_CALLBACK_ID,
            "private_metadata": json.dumps({
                "message_ts": message_ts,
                "channel_id": body["channel"]["id"],
            }),
            "title": {"type": "plain_text", "text": "Modify action"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "modification_block",
                    "label": {"type": "plain_text", "text": "What should change?"},
                    "element": {
                        "type": "plain_text_input",
                        "multiline": True,
                        "action_id": "modification_text",
                    },
                }
            ],
        },
    )


def submit_modify_action(
    *,
    body: dict[str, Any],
    client: Any,
    token_store: TokenStore | None = None,
    decision_log: DecisionLog | None = None,
    stepfunctions_client: Any | None = None,
) -> None:
    token_store = token_store or TokenStore()
    decision_log = decision_log or DecisionLog()
    stepfunctions_client = stepfunctions_client or _stepfunctions_client()
    metadata = json.loads(body["view"]["private_metadata"])
    message_ts = str(metadata["message_ts"])
    modification = body["view"]["state"]["values"]["modification_block"]["modification_text"]["value"]
    token_record = token_store.retrieve_token(message_ts)
    if token_record is None:
        raise KeyError(f"No task token found for Slack message {message_ts}")
    stepfunctions_client.send_task_success(
        taskToken=token_record.task_token,
        output=json.dumps({"decision": "modified", "modification": modification}),
    )
    client.chat_update(
        channel=metadata["channel_id"],
        ts=message_ts,
        text="Action modified",
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Modified and returned to workflow.\n>{modification}"},
            }
        ],
    )
    decision_log.write(
        {
            "customer_id": token_record.customer_id,
            "action_type": token_record.action_type,
            "recommended_action": {"message_ts": message_ts},
            "human_decision": "modified",
            "modified_action": {"instructions": modification},
            "slack_message_ts": message_ts,
            "tool_call_inputs": body,
            "tool_call_outputs": {"decision": "modified", "instructions": modification},
        }
    )


def dismiss_action(
    *,
    body: dict[str, Any],
    client: Any,
    token_store: TokenStore | None = None,
    decision_log: DecisionLog | None = None,
    stepfunctions_client: Any | None = None,
) -> None:
    token_store = token_store or TokenStore()
    decision_log = decision_log or DecisionLog()
    stepfunctions_client = stepfunctions_client or _stepfunctions_client()
    message_ts = _message_ts_from_body(body)
    token_record = token_store.retrieve_token(message_ts)
    if token_record is None:
        raise KeyError(f"No task token found for Slack message {message_ts}")
    stepfunctions_client.send_task_success(
        taskToken=token_record.task_token,
        output=json.dumps({"decision": "dismissed"}),
    )
    client.chat_update(
        channel=body["channel"]["id"],
        ts=message_ts,
        text="Action dismissed",
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Dismissed. No action will be executed for this alert."},
            }
        ],
    )
    decision_log.write(_decision_log_record(token_record.customer_id, token_record.action_type, "dismissed", body))
