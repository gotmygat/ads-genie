from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.config.thresholds import NEGATIVE_KEYWORD_MAX_CONVERSIONS, NEGATIVE_KEYWORD_MIN_SPEND
from mcp_server.config.verticals import VERTICALS, VerticalConfig
from mcp_server.gaql.queries import (
    AD_GROUP_QUALITY_SCORES,
    QueryExecutor,
    campaign_performance_window,
    search_terms_window,
)


class WastedCampaign(BaseModel):
    campaign_id: str
    campaign_name: str
    total_spend: float
    conversions: float
    waste_category: Literal["zero_conversion", "low_quality_score", "irrelevant_terms", "misallocated_budget"]
    estimated_recoverable_spend: float


class BudgetWasteResult(BaseModel):
    total_wasted_spend: float
    wasted_campaigns: list[WastedCampaign]
    top_irrelevant_search_terms: list[dict[str, Any]]
    misallocated_budget_campaigns: list[dict[str, Any]]
    waste_pct_of_total_spend: float


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


def analyze_budget_waste(
    customer_id: str,
    vertical: str,
    lookback_days: int = 30,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
) -> BudgetWasteResult:
    """
    Encodes Render's four-signal waste definition: zero-conversion spend, irrelevant search
    terms, quality-score-driven CPC drag, and budget misallocation on converting campaigns.
    """

    vertical_config = _require_vertical(vertical)
    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)

    campaigns = query_executor.run(client, customer_id, campaign_performance_window(lookback_days))
    quality_rows = query_executor.run(client, customer_id, AD_GROUP_QUALITY_SCORES)
    search_terms = query_executor.run(client, customer_id, search_terms_window(lookback_days))

    wasted_campaigns: list[WastedCampaign] = []
    top_irrelevant_search_terms: list[dict[str, Any]] = []
    misallocated_budget_campaigns: list[dict[str, Any]] = []

    total_spend = sum(_metric_value(row, "metrics", "cost", default=0.0) for row in campaigns)
    zero_conversion_threshold = vertical_config.waste_cost_threshold_daily * lookback_days

    for row in campaigns:
        campaign_id = str(row.get("campaign", {}).get("id", ""))
        campaign_name = str(row.get("campaign", {}).get("name", "unknown"))
        spend = _metric_value(row, "metrics", "cost", default=0.0)
        conversions = _metric_value(row, "metrics", "conversions", default=0.0)
        revenue = _metric_value(row, "metrics", "all_conversions_value", default=0.0)
        lost_budget = _metric_value(row, "metrics", "search_budget_lost_impression_share", default=0.0)
        roas = revenue / spend if spend else 0.0

        if spend >= zero_conversion_threshold and conversions <= 0:
            wasted_campaigns.append(
                WastedCampaign(
                    campaign_id=campaign_id,
                    campaign_name=campaign_name,
                    total_spend=round(spend, 2),
                    conversions=conversions,
                    waste_category="zero_conversion",
                    estimated_recoverable_spend=round(spend * 0.8, 2),
                )
            )

        if lost_budget > 0.25 and roas > vertical_config.min_acceptable_roas:
            recoverable = spend * min(lost_budget, 0.5)
            wasted_campaigns.append(
                WastedCampaign(
                    campaign_id=campaign_id,
                    campaign_name=campaign_name,
                    total_spend=round(spend, 2),
                    conversions=conversions,
                    waste_category="misallocated_budget",
                    estimated_recoverable_spend=round(recoverable, 2),
                )
            )
            misallocated_budget_campaigns.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "search_budget_lost_impression_share": round(lost_budget, 4),
                    "roas": round(roas, 2),
                    "estimated_recoverable_spend": round(recoverable, 2),
                }
            )

    quality_costs: dict[str, tuple[str, float, float]] = {}
    for row in quality_rows:
        campaign_id = str(row.get("campaign", {}).get("id", ""))
        quality_score = _metric_value(row, "ad_group_criterion", "quality_info", "quality_score", default=0.0)
        spend = _metric_value(row, "metrics", "cost", default=0.0)
        conversions = _metric_value(row, "metrics", "conversions", default=0.0)
        campaign_name = str(row.get("campaign", {}).get("name", "unknown"))
        if quality_score >= 5 or spend <= 0:
            continue
        cost_drag = spend * ((7 - quality_score) / 7) * 0.15
        existing = quality_costs.get(campaign_id, (campaign_name, 0.0, 0.0))
        quality_costs[campaign_id] = (campaign_name, existing[1] + cost_drag, existing[2] + conversions)

    for campaign_id, (campaign_name, cost_drag, conversions) in quality_costs.items():
        wasted_campaigns.append(
            WastedCampaign(
                campaign_id=campaign_id,
                campaign_name=campaign_name,
                total_spend=round(cost_drag, 2),
                conversions=round(conversions, 2),
                waste_category="low_quality_score",
                estimated_recoverable_spend=round(cost_drag, 2),
            )
        )

    irrelevant_spend_by_campaign: dict[str, tuple[str, float]] = {}
    for row in search_terms:
        term = str(row.get("search_term_view", {}).get("search_term", ""))
        spend = _metric_value(row, "metrics", "cost", default=0.0)
        conversions = _metric_value(row, "metrics", "conversions", default=0.0)
        campaign_id = str(row.get("campaign", {}).get("id", ""))
        campaign_name = str(row.get("campaign", {}).get("name", "unknown"))
        if spend < NEGATIVE_KEYWORD_MIN_SPEND or conversions > NEGATIVE_KEYWORD_MAX_CONVERSIONS:
            continue

        top_irrelevant_search_terms.append(
            {
                "search_term": term,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "spend": round(spend, 2),
                "conversions": conversions,
            }
        )
        existing = irrelevant_spend_by_campaign.get(campaign_id, (campaign_name, 0.0))
        irrelevant_spend_by_campaign[campaign_id] = (campaign_name, existing[1] + spend)

    for campaign_id, (campaign_name, irrelevant_spend) in irrelevant_spend_by_campaign.items():
        wasted_campaigns.append(
            WastedCampaign(
                campaign_id=campaign_id,
                campaign_name=campaign_name,
                total_spend=round(irrelevant_spend, 2),
                conversions=0.0,
                waste_category="irrelevant_terms",
                estimated_recoverable_spend=round(irrelevant_spend, 2),
            )
        )

    total_wasted_spend = round(sum(item.estimated_recoverable_spend for item in wasted_campaigns), 2)
    waste_pct_of_total_spend = round((total_wasted_spend / total_spend) * 100, 2) if total_spend else 0.0
    top_irrelevant_search_terms.sort(key=lambda item: item["spend"], reverse=True)

    return BudgetWasteResult(
        total_wasted_spend=total_wasted_spend,
        wasted_campaigns=wasted_campaigns,
        top_irrelevant_search_terms=top_irrelevant_search_terms[:10],
        misallocated_budget_campaigns=misallocated_budget_campaigns,
        waste_pct_of_total_spend=waste_pct_of_total_spend,
    )
