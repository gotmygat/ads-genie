from __future__ import annotations

from typing import Any, Literal
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]
from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.config.verticals import VERTICALS
from mcp_server.gaql.queries import CAMPAIGN_PERFORMANCE_7D, QueryExecutor


class BenchmarkResult(BaseModel):
    account_roas: float
    vertical_avg_roas: float
    roas_percentile: float
    account_cpa: float
    vertical_avg_cpa: float
    cpa_percentile: float
    account_ctr: float
    vertical_avg_ctr: float
    ctr_percentile: float
    overall_rank: Literal["top_quartile", "above_avg", "below_avg", "bottom_quartile"]
    improvement_opportunities: list[str]


def _metric_value(row: dict[str, Any], *path: str, default: float = 0.0) -> float:
    current: Any = row
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    try:
        return float(current)
    except (TypeError, ValueError):
        return default


def _load_vertical_benchmark(vertical: str, loader: Any | None = None) -> dict[str, Any]:
    if loader is not None:
        return loader(vertical)
    if boto3 is None:
        return {}
    table_name = os.getenv("DYNAMODB_MCC_BENCHMARKS_TABLE", "ads_genie_mcc_benchmarks")
    table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")).Table(table_name)
    item = table.get_item(Key={"vertical": vertical}).get("Item", {})
    return item


def _percentile(account_value: float, values: list[float], reverse: bool = False) -> float:
    if not values:
        return 50.0
    ordered = sorted(values, reverse=reverse)
    below = sum(1 for value in ordered if value <= account_value) if not reverse else sum(1 for value in ordered if value >= account_value)
    return round((below / len(ordered)) * 100.0, 2)


def benchmark_account(
    customer_id: str,
    vertical: str,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
    benchmark_loader: Any | None = None,
) -> BenchmarkResult:
    if vertical not in VERTICALS:
        raise ValueError(f"Unknown vertical '{vertical}'")

    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)
    rows = query_executor.run(client, customer_id, CAMPAIGN_PERFORMANCE_7D)
    benchmark = _load_vertical_benchmark(vertical, loader=benchmark_loader)

    sample_size = int(benchmark.get("account_count", 0))
    account_cost = sum(_metric_value(row, "metrics", "cost", default=0.0) for row in rows)
    account_value = sum(
        _metric_value(row, "metrics", "all_conversions_value", default=_metric_value(row, "metrics", "conversions", default=0.0))
        for row in rows
    )
    account_conversions = sum(_metric_value(row, "metrics", "conversions", default=0.0) for row in rows)
    account_clicks = sum(_metric_value(row, "metrics", "clicks", default=0.0) for row in rows)
    account_impressions = sum(_metric_value(row, "metrics", "impressions", default=0.0) for row in rows)

    account_roas = account_value / account_cost if account_cost else 0.0
    account_cpa = account_cost / account_conversions if account_conversions else 0.0
    account_ctr = account_clicks / account_impressions if account_impressions else 0.0

    vertical_avg_roas = float(benchmark.get("avg_roas", 0.0))
    vertical_avg_cpa = float(benchmark.get("avg_cpa", 0.0))
    vertical_avg_ctr = float(benchmark.get("avg_ctr", 0.0))

    roas_distribution = [float(value) for value in benchmark.get("roas_distribution", [vertical_avg_roas])]
    cpa_distribution = [float(value) for value in benchmark.get("cpa_distribution", [vertical_avg_cpa])]
    ctr_distribution = [float(value) for value in benchmark.get("ctr_distribution", [vertical_avg_ctr])]

    opportunities: list[str] = []
    if sample_size < 3:
        opportunities.append(f"Insufficient benchmark data for vertical '{vertical}' (need at least 3 accounts).")

    if vertical_avg_roas and account_roas < vertical_avg_roas:
        opportunities.append("ROAS is below vertical average; inspect wasted spend and conversion efficiency.")
    if vertical_avg_cpa and account_cpa > vertical_avg_cpa:
        opportunities.append("CPA is above vertical average; tighten targeting and landing page conversion path.")
    if vertical_avg_ctr and account_ctr < vertical_avg_ctr:
        opportunities.append("CTR is below vertical average; revisit ad copy and keyword/ad-group tightness.")

    roas_percentile = _percentile(account_roas, roas_distribution)
    cpa_percentile = _percentile(account_cpa, cpa_distribution, reverse=True)
    ctr_percentile = _percentile(account_ctr, ctr_distribution)
    blended = (roas_percentile + cpa_percentile + ctr_percentile) / 3
    if blended >= 75:
        rank: Literal["top_quartile", "above_avg", "below_avg", "bottom_quartile"] = "top_quartile"
    elif blended >= 55:
        rank = "above_avg"
    elif blended >= 30:
        rank = "below_avg"
    else:
        rank = "bottom_quartile"

    return BenchmarkResult(
        account_roas=round(account_roas, 4),
        vertical_avg_roas=round(vertical_avg_roas, 4),
        roas_percentile=roas_percentile,
        account_cpa=round(account_cpa, 4),
        vertical_avg_cpa=round(vertical_avg_cpa, 4),
        cpa_percentile=cpa_percentile,
        account_ctr=round(account_ctr, 4),
        vertical_avg_ctr=round(vertical_avg_ctr, 4),
        ctr_percentile=ctr_percentile,
        overall_rank=rank,
        improvement_opportunities=opportunities or ["No material benchmark gaps detected."],
    )
