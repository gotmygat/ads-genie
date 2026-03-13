from __future__ import annotations

import os

import boto3
from moto import mock_aws

from mcp_server.cache.dynamodb_cache import DynamoDBCache
from mcp_server.gaql.queries import QueryExecutor
from mcp_server.tools.health_check import health_check


class FakeGoogleAdsService:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def search(self, customer_id: str, query: str):
        del customer_id
        self.calls.append(query)
        if "FROM campaign" in query:
            return [
                {
                    "campaign": {"id": "111", "name": "Campaign A", "status": "ENABLED"},
                    "metrics": {
                        "cost_micros": 100_000_000,
                        "conversions": 10.0,
                        "all_conversions_value": 400.0,
                        "impressions": 1_000.0,
                        "clicks": 100.0,
                        "search_budget_lost_impression_share": 0.1,
                    },
                }
            ]
        return [{"ad_group_criterion": {"quality_info": {"quality_score": 7}}}]


class FakeClient:
    def __init__(self, service: FakeGoogleAdsService) -> None:
        self.service = service

    def get_service(self, name: str):
        assert name == "GoogleAdsService"
        return self.service


class FakeAuth:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def get_client(self, customer_id: str):  # noqa: ANN001
        del customer_id
        return self.client


@mock_aws

def test_health_check_tool_uses_cache_and_writes_dynamodb(monkeypatch) -> None:
    region = "us-east-1"
    table_name = "ads_genie_cache"
    monkeypatch.setenv("AWS_REGION", region)
    monkeypatch.setenv("DYNAMODB_CACHE_TABLE", table_name)

    dynamodb = boto3.client("dynamodb", region_name=region)
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "cache_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "cache_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    service = FakeGoogleAdsService()
    auth = FakeAuth(FakeClient(service))
    query_executor = QueryExecutor(cache=DynamoDBCache(table_name=table_name, region_name=region))

    first = health_check("123", "day_spa", query_executor=query_executor, auth=auth)
    second = health_check("123", "day_spa", query_executor=query_executor, auth=auth)

    assert first.overall_status == "healthy"
    assert second.overall_status == "healthy"
    assert len(service.calls) == 2

    items = boto3.resource("dynamodb", region_name=region).Table(table_name).scan().get("Items", [])
    assert len(items) == 2
