from __future__ import annotations

import boto3
from moto import mock_aws

from orchestration.models.decision_log import DecisionLog
from slack_bot.handlers.approval_handler import approve_action
from slack_bot.task_token_bridge.token_store import TokenStore


class FakeSlackClient:
    def __init__(self) -> None:
        self.updates: list[dict] = []

    def chat_update(self, **kwargs):
        self.updates.append(kwargs)


class FakeStepFunctionsClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send_task_success(self, **kwargs):
        self.calls.append(kwargs)


@mock_aws

def test_approve_action_resolves_task_token_and_writes_decision(monkeypatch) -> None:
    region = "us-east-1"
    token_table = "ads_genie_task_tokens"
    decisions_table = "ads_genie_decisions"
    monkeypatch.setenv("AWS_REGION", region)
    monkeypatch.setenv("DYNAMODB_TASK_TOKENS_TABLE", token_table)
    monkeypatch.setenv("DYNAMODB_DECISIONS_TABLE", decisions_table)

    dynamodb = boto3.client("dynamodb", region_name=region)
    dynamodb.create_table(
        TableName=token_table,
        KeySchema=[{"AttributeName": "message_ts", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "message_ts", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.create_table(
        TableName=decisions_table,
        KeySchema=[
            {"AttributeName": "decision_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "decision_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    token_store = TokenStore(table_name=token_table, region_name=region)
    token_store.store_token(
        message_ts="1700000000.000100",
        execution_arn="arn:aws:states:execution:test",
        task_token="task-token-1",
        customer_id="123",
        action_type="adjust_bids",
    )

    body = {
        "channel": {"id": "C123"},
        "container": {"message_ts": "1700000000.000100"},
        "actions": [{"value": "{}"}],
    }
    slack_client = FakeSlackClient()
    stepfunctions_client = FakeStepFunctionsClient()
    decision_log = DecisionLog(table_name=decisions_table, bucket_name=None, region_name=region)

    approve_action(
        body=body,
        client=slack_client,
        token_store=token_store,
        decision_log=decision_log,
        stepfunctions_client=stepfunctions_client,
    )

    assert stepfunctions_client.calls[0]["taskToken"] == "task-token-1"
    assert slack_client.updates[0]["channel"] == "C123"
    items = boto3.resource("dynamodb", region_name=region).Table(decisions_table).scan().get("Items", [])
    assert len(items) == 1
    assert items[0]["human_decision"] == "approved"
