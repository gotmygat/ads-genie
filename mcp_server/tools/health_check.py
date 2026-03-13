from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.config.thresholds import IMPRESSION_SHARE_LOST_TO_BUDGET_PCT
from mcp_server.config.verticals import VERTICALS, VerticalConfig
from mcp_server.gaql.queries import AD_GROUP_QUALITY_SCORES, CAMPAIGN_PERFORMANCE_7D, QueryExecutor


class HealthCheckResult(BaseModel):
    overall_status: Literal["healthy", "warning", "critical"]
    roas_status: str
    impression_share_status: str
    quality_score_status: str
    zero_conversion_campaigns: list[str]
    budget_utilization_pct: float
    recommended_actions: list[str]
    vertical_benchmarks_used: dict[str, Any]
    checked_at: datetime


def _require_vertical(vertical: str) -> VerticalConfig:
    try:
        return VERTICALS[vertical]
    except KeyError as exc:  # pragma: no cover - exercised in unit tests
        raise ValueError(f"Unknown vertical '{vertical}'. Expected one of: {', '.join(sorted(VERTICALS))}") from exc


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


def health_check(
    customer_id: str,
    vertical: str,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
) -> HealthCheckResult:
    """
    Encodes Render's vertical-aware health review: ROAS against benchmark, budget starvation,
    quality score deterioration, and wasted zero-conversion campaign spend.
    """

    vertical_config = _require_vertical(vertical)
    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)

    campaigns = query_executor.run(client, customer_id, CAMPAIGN_PERFORMANCE_7D)
    quality_rows = query_executor.run(client, customer_id, AD_GROUP_QUALITY_SCORES)

    total_cost = sum(_metric_value(row, "metrics", "cost", default=0.0) for row in campaigns)
    total_value = sum(
        _metric_value(row, "metrics", "all_conversions_value", default=_metric_value(row, "metrics", "conversions", default=0.0))
        for row in campaigns
    )
    avg_roas = total_value / total_cost if total_cost else 0.0

    zero_conversion_threshold = vertical_config.waste_cost_threshold_daily * 7
    zero_conversion_campaigns = [
        row.get("campaign", {}).get("name", "unknown")
        for row in campaigns
        if _metric_value(row, "metrics", "cost", default=0.0) >= zero_conversion_threshold
        and _metric_value(row, "metrics", "conversions", default=0.0) <= 0
    ]

    quality_scores = [
        _metric_value(row, "ad_group_criterion", "quality_info", "quality_score", default=0.0)
        for row in quality_rows
        if _metric_value(row, "ad_group_criterion", "quality_info", "quality_score", default=0.0) > 0
    ]
    avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0

    lost_budget_values = [
        _metric_value(row, "metrics", "search_budget_lost_impression_share", default=0.0) for row in campaigns
    ]
    avg_lost_budget = sum(lost_budget_values) / len(lost_budget_values) if lost_budget_values else 0.0
    budget_utilization_pct = round(max(0.0, 100.0 - (avg_lost_budget * 100.0)), 2)

    recommended_actions: list[str] = []
    roas_status = f"ROAS {avg_roas:.2f} vs minimum {vertical_config.min_acceptable_roas:.2f}"
    quality_score_status = (
        f"Average quality score {avg_quality_score:.2f} vs minimum {vertical_config.min_quality_score}"
    )
    impression_share_status = (
        f"Lost budget impression share {avg_lost_budget * 100:.1f}% vs threshold "
        f"{IMPRESSION_SHARE_LOST_TO_BUDGET_PCT:.1f}%"
    )

    status = "healthy"
    if avg_roas < vertical_config.min_acceptable_roas:
        status = "warning"
        recommended_actions.append("Investigate conversion value efficiency and budget allocation.")
    if avg_roas < vertical_config.min_acceptable_roas * 0.7:
        status = "critical"

    if avg_quality_score and avg_quality_score < vertical_config.min_quality_score:
        status = "warning" if status == "healthy" else status
        recommended_actions.append("Improve keyword/ad relevance and landing page alignment to raise quality score.")
    if avg_quality_score and avg_quality_score < max(1, vertical_config.min_quality_score - 2):
        status = "critical"

    if avg_lost_budget * 100 > IMPRESSION_SHARE_LOST_TO_BUDGET_PCT:
        status = "warning" if status == "healthy" else status
        recommended_actions.append("Budget-starved campaigns need reallocation or budget increase.")
    if avg_lost_budget * 100 > IMPRESSION_SHARE_LOST_TO_BUDGET_PCT * 1.5:
        status = "critical"

    if zero_conversion_campaigns:
        status = "warning" if status == "healthy" else status
        recommended_actions.append("Pause or restructure campaigns with sustained spend and zero conversions.")
    if len(zero_conversion_campaigns) >= 2:
        status = "critical"

    if not recommended_actions:
        recommended_actions.append("No immediate action required; account is within configured thresholds.")

    return HealthCheckResult(
        overall_status=status,
        roas_status=roas_status,
        impression_share_status=impression_share_status,
        quality_score_status=quality_score_status,
        zero_conversion_campaigns=zero_conversion_campaigns,
        budget_utilization_pct=budget_utilization_pct,
        recommended_actions=recommended_actions,
        vertical_benchmarks_used=vertical_config.__dict__,
        checked_at=datetime.now(timezone.utc),
    )
