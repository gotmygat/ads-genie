from __future__ import annotations

import asyncio
from statistics import mean, pstdev
from typing import Any, Literal
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]
from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.gaql.queries import QueryExecutor, campaign_performance_by_day_window


class AccountAnomaly(BaseModel):
    customer_id: str
    account_name: str
    vertical: str
    anomaly_type: Literal["cost_spike", "roas_drop", "conversion_collapse", "quality_score_drop"]
    severity: Literal["low", "medium", "high", "critical"]
    current_value: float
    baseline_value: float
    deviation_pct: float


class CrossMCCReport(BaseModel):
    anomalies: list[AccountAnomaly]


def _load_accounts(vertical: str | None = None, loader: Any | None = None) -> list[dict[str, Any]]:
    if loader is not None:
        return loader(vertical)
    if boto3 is None:
        return []
    table_name = os.getenv("DYNAMODB_ACCOUNTS_TABLE", "").strip()
    if not table_name:
        return []
    table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")).Table(table_name)
    items = table.scan().get("Items", [])
    filtered = [item for item in items if item.get("is_active", True)]
    if vertical:
        filtered = [item for item in filtered if item.get("vertical") == vertical]
    return filtered


def _severity_from_deviation(deviation_pct: float) -> Literal["low", "medium", "high", "critical"]:
    if deviation_pct >= 60:
        return "critical"
    if deviation_pct >= 40:
        return "high"
    if deviation_pct >= 25:
        return "medium"
    return "low"


async def _snapshot_account(account: dict[str, Any], query_executor: QueryExecutor, auth: GoogleAdsAuth) -> dict[str, Any]:
    customer_id = str(account["customer_id"])
    client = auth.get_client(customer_id)
    rows = query_executor.run(client, customer_id, campaign_performance_by_day_window(14))
    daily_costs: list[float] = []
    daily_values: list[float] = []
    daily_conversions: list[float] = []
    for row in rows:
        metrics = row.get("metrics", {})
        daily_costs.append(float(metrics.get("cost", 0.0)))
        daily_values.append(float(metrics.get("all_conversions_value", metrics.get("conversions", 0.0))))
        daily_conversions.append(float(metrics.get("conversions", 0.0)))

    return {
        "account": account,
        "daily_costs": daily_costs,
        "daily_values": daily_values,
        "daily_conversions": daily_conversions,
    }


def cross_mcc_anomalies(
    vertical: str | None = None,
    account_loader: Any | None = None,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
) -> CrossMCCReport:
    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    accounts = _load_accounts(vertical, loader=account_loader)

    async def runner() -> list[dict[str, Any]]:
        tasks = [_snapshot_account(account, query_executor, auth) for account in accounts]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    snapshots = asyncio.run(runner())
    anomalies: list[AccountAnomaly] = []
    for snapshot in snapshots:
        costs = snapshot["daily_costs"]
        values = snapshot["daily_values"]
        conversions = snapshot["daily_conversions"]
        if len(costs) < 8:
            continue

        current_cost = costs[-1]
        baseline_cost = mean(costs[:-1]) if costs[:-1] else 0.0
        cost_std = pstdev(costs[:-1]) if len(costs[:-1]) > 1 else 0.0
        if baseline_cost and cost_std and current_cost > baseline_cost + (2 * cost_std):
            deviation_pct = ((current_cost - baseline_cost) / baseline_cost) * 100
            anomalies.append(
                AccountAnomaly(
                    customer_id=str(snapshot["account"]["customer_id"]),
                    account_name=str(snapshot["account"].get("account_name", snapshot["account"].get("descriptive_name", "unknown"))),
                    vertical=str(snapshot["account"].get("vertical", "unknown")),
                    anomaly_type="cost_spike",
                    severity=_severity_from_deviation(deviation_pct),
                    current_value=round(current_cost, 2),
                    baseline_value=round(baseline_cost, 2),
                    deviation_pct=round(deviation_pct, 2),
                )
            )

        current_roas = values[-1] / costs[-1] if costs[-1] else 0.0
        baseline_roas_values = [
            (value / cost) for value, cost in zip(values[:-1], costs[:-1], strict=False) if cost
        ]
        baseline_roas = mean(baseline_roas_values) if baseline_roas_values else 0.0
        roas_std = pstdev(baseline_roas_values) if len(baseline_roas_values) > 1 else 0.0
        if baseline_roas and roas_std and current_roas < baseline_roas - (2 * roas_std):
            deviation_pct = ((baseline_roas - current_roas) / baseline_roas) * 100
            anomalies.append(
                AccountAnomaly(
                    customer_id=str(snapshot["account"]["customer_id"]),
                    account_name=str(snapshot["account"].get("account_name", snapshot["account"].get("descriptive_name", "unknown"))),
                    vertical=str(snapshot["account"].get("vertical", "unknown")),
                    anomaly_type="roas_drop",
                    severity=_severity_from_deviation(deviation_pct),
                    current_value=round(current_roas, 2),
                    baseline_value=round(baseline_roas, 2),
                    deviation_pct=round(deviation_pct, 2),
                )
            )

        current_conv = conversions[-1]
        baseline_conv = mean(conversions[:-1]) if conversions[:-1] else 0.0
        conv_std = pstdev(conversions[:-1]) if len(conversions[:-1]) > 1 else 0.0
        if baseline_conv and conv_std and current_conv < baseline_conv - (2 * conv_std):
            deviation_pct = ((baseline_conv - current_conv) / baseline_conv) * 100
            anomalies.append(
                AccountAnomaly(
                    customer_id=str(snapshot["account"]["customer_id"]),
                    account_name=str(snapshot["account"].get("account_name", snapshot["account"].get("descriptive_name", "unknown"))),
                    vertical=str(snapshot["account"].get("vertical", "unknown")),
                    anomaly_type="conversion_collapse",
                    severity=_severity_from_deviation(deviation_pct),
                    current_value=round(current_conv, 2),
                    baseline_value=round(baseline_conv, 2),
                    deviation_pct=round(deviation_pct, 2),
                )
            )

    return CrossMCCReport(anomalies=anomalies)
