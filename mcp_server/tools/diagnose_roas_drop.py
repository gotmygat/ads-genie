from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Literal

from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.config.verticals import VERTICALS, VerticalConfig
from mcp_server.gaql.queries import (
    AD_GROUP_QUALITY_SCORES,
    AUCTION_INSIGHTS,
    QueryExecutor,
    campaign_performance_by_day_window,
    campaign_performance_same_period_last_year,
    search_terms_window,
)


class RoasDiagnosis(BaseModel):
    roas_current_week: float
    roas_prior_week: float
    drop_pct: float
    primary_cause: str
    supporting_evidence: list[str]
    recommended_fix: str
    confidence: Literal["high", "medium", "low"]
    playbook_steps_executed: list[str]


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


def diagnose_roas_drop(
    customer_id: str,
    vertical: str,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
) -> RoasDiagnosis:
    """
    Executes Render's ordered ROAS triage playbook:
    search terms -> auction insights -> quality score -> landing-page proxy -> seasonality.
    The first materially degraded signal becomes the primary cause.
    """

    _require_vertical(vertical)
    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)

    performance_rows = query_executor.run(client, customer_id, campaign_performance_by_day_window(14))
    search_term_rows = query_executor.run(client, customer_id, search_terms_window(30))
    auction_rows = query_executor.run(client, customer_id, AUCTION_INSIGHTS)
    quality_rows = query_executor.run(client, customer_id, AD_GROUP_QUALITY_SCORES)
    prior_year_rows = query_executor.run(client, customer_id, campaign_performance_same_period_last_year(14))

    today = date.today()
    current_start = today - timedelta(days=7)
    prior_start = today - timedelta(days=14)
    prior_end = today - timedelta(days=8)

    current_cost = current_value = current_clicks = current_conversions = 0.0
    prior_cost = prior_value = prior_clicks = prior_conversions = 0.0
    for row in performance_rows:
        day_value = str(row.get("segments", {}).get("date", ""))
        try:
            day = date.fromisoformat(day_value)
        except ValueError:
            continue
        cost = _metric_value(row, "metrics", "cost", default=0.0)
        value = _metric_value(row, "metrics", "all_conversions_value", default=0.0)
        clicks = _metric_value(row, "metrics", "clicks", default=0.0)
        conversions = _metric_value(row, "metrics", "conversions", default=0.0)
        if current_start <= day <= today - timedelta(days=1):
            current_cost += cost
            current_value += value
            current_clicks += clicks
            current_conversions += conversions
        elif prior_start <= day <= prior_end:
            prior_cost += cost
            prior_value += value
            prior_clicks += clicks
            prior_conversions += conversions

    roas_current_week = current_value / current_cost if current_cost else 0.0
    roas_prior_week = prior_value / prior_cost if prior_cost else 0.0
    drop_pct = ((roas_prior_week - roas_current_week) / roas_prior_week * 100.0) if roas_prior_week else 0.0

    playbook_steps = [
        "check_search_terms",
        "check_auction_insights",
        "check_quality_scores",
        "check_landing_page_proxy",
        "check_seasonality",
    ]
    supporting_evidence: list[str] = []
    primary_cause = "no_material_change_detected"
    recommended_fix = "Continue monitoring; no single dominant degradation factor found."
    confidence: Literal["high", "medium", "low"] = "low"

    # Significant means irrelevant zero-conversion term spend > $100 in the last 7 days.
    recent_irrelevant_spend = sum(
        _metric_value(row, "metrics", "cost", default=0.0)
        for row in search_term_rows
        if _metric_value(row, "metrics", "conversions", default=0.0) <= 0
        and _metric_value(row, "metrics", "cost", default=0.0) >= 5.0
    )
    if recent_irrelevant_spend > 100:
        primary_cause = "irrelevant_search_terms"
        supporting_evidence.append(
            f"Irrelevant search term spend reached ${recent_irrelevant_spend:.2f} in the last 30 days."
        )
        recommended_fix = "Review recent search terms and add negatives at campaign/ad-group scope."
        confidence = "high"
    else:
        # Significant means at least one auction insight domain with impression_share >= 0.10.
        competitor_domains = [
            row.get("auction_insight", {}).get("domain", "")
            for row in auction_rows
            if _metric_value(row, "auction_insight", "impression_share", default=0.0) >= 0.10
        ]
        if competitor_domains:
            primary_cause = "competitor_entry"
            supporting_evidence.append(
                f"New/high-overlap competitor domains detected: {', '.join(sorted(set(competitor_domains))[:3])}."
            )
            recommended_fix = "Audit auction pressure, update bids selectively, and strengthen ad relevance."
            confidence = "medium"
        else:
            # Significant means average quality score falls by >= 1.0 points versus baseline target of 7.
            quality_scores = [
                _metric_value(row, "ad_group_criterion", "quality_info", "quality_score", default=0.0)
                for row in quality_rows
                if _metric_value(row, "ad_group_criterion", "quality_info", "quality_score", default=0.0) > 0
            ]
            avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
            if avg_quality and avg_quality <= 6.0:
                primary_cause = "quality_score_drop"
                supporting_evidence.append(f"Average keyword quality score fell to {avg_quality:.2f}.")
                recommended_fix = "Tighten keyword-to-ad-theme alignment and audit landing page relevance."
                confidence = "medium"
            else:
                # Significant means click-to-conversion ratio worsens by >= 20% week over week.
                current_click_to_conv = current_conversions / current_clicks if current_clicks else 0.0
                prior_click_to_conv = prior_conversions / prior_clicks if prior_clicks else 0.0
                click_to_conv_drop = (
                    ((prior_click_to_conv - current_click_to_conv) / prior_click_to_conv) * 100.0
                    if prior_click_to_conv
                    else 0.0
                )
                if click_to_conv_drop >= 20:
                    primary_cause = "landing_page_conversion_drop"
                    supporting_evidence.append(
                        f"Click-to-conversion efficiency declined {click_to_conv_drop:.1f}% week over week."
                    )
                    recommended_fix = "Inspect landing page speed, form friction, and offer/message alignment."
                    confidence = "medium"
                else:
                    # Significant means current value is >= 15% below same-period historical proxy.
                    prior_year_value = sum(
                        _metric_value(row, "metrics", "all_conversions_value", default=0.0)
                        for row in prior_year_rows
                    )
                    if prior_year_value and current_value <= prior_year_value * 0.85:
                        primary_cause = "seasonality"
                        supporting_evidence.append(
                            "Current conversion value is materially below same-period historical baseline."
                        )
                        recommended_fix = "Adjust expectations and bidding/budget strategy for seasonal demand."
                        confidence = "low"

    if not supporting_evidence:
        supporting_evidence.append("ROAS decline was observed, but no playbook step crossed the significance threshold.")

    return RoasDiagnosis(
        roas_current_week=round(roas_current_week, 4),
        roas_prior_week=round(roas_prior_week, 4),
        drop_pct=round(drop_pct, 2),
        primary_cause=primary_cause,
        supporting_evidence=supporting_evidence,
        recommended_fix=recommended_fix,
        confidence=confidence,
        playbook_steps_executed=playbook_steps,
    )
