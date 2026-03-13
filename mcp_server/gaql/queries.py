from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any
import re

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:  # pragma: no cover - dependency installed in target runtime
    MessageToDict = None  # type: ignore[assignment]

from mcp_server.cache.dynamodb_cache import DynamoDBCache


CAMPAIGN_PERFORMANCE_7D = """
    SELECT campaign.id, campaign.name, campaign.status,
           metrics.cost_micros, metrics.conversions,
           metrics.impressions, metrics.clicks,
           metrics.search_impression_share,
           metrics.search_budget_lost_impression_share
    FROM campaign
    WHERE segments.date DURING LAST_7_DAYS
    AND campaign.status = 'ENABLED'
"""

AD_GROUP_QUALITY_SCORES = """
    SELECT ad_group.id, ad_group.name, campaign.id,
           ad_group_criterion.quality_info.quality_score,
           ad_group_criterion.keyword.text,
           metrics.cost_micros, metrics.conversions
    FROM ad_group_criterion
    WHERE ad_group_criterion.type = 'KEYWORD'
    AND segments.date DURING LAST_7_DAYS
"""

SEARCH_TERMS_REPORT = """
    SELECT search_term_view.search_term,
           campaign.id, campaign.name,
           ad_group.id,
           metrics.cost_micros, metrics.conversions,
           metrics.impressions, metrics.clicks
    FROM search_term_view
    WHERE segments.date DURING LAST_30_DAYS
    AND metrics.cost_micros > 0
"""

CAMPAIGN_PERFORMANCE_WOW = """
    SELECT campaign.id, campaign.name,
           metrics.cost_micros, metrics.conversions,
           metrics.all_conversions_value
    FROM campaign
    WHERE segments.date DURING LAST_14_DAYS
    AND campaign.status = 'ENABLED'
"""

AUCTION_INSIGHTS = """
    SELECT campaign.id,
           auction_insight.domain,
           auction_insight.impression_share,
           auction_insight.position_above_rate,
           auction_insight.overlap_rate
    FROM auction_insight_campaign_date_range
    WHERE segments.date DURING LAST_30_DAYS
"""

KEYWORD_PERFORMANCE = """
    SELECT ad_group_criterion.keyword.text,
           ad_group_criterion.keyword.match_type,
           campaign.id, ad_group.id,
           metrics.cost_micros, metrics.conversions,
           metrics.quality_score,
           metrics.search_rank_lost_impression_share
    FROM keyword_view
    WHERE segments.date DURING LAST_30_DAYS
"""


def campaign_performance_window(lookback_days: int) -> str:
    return f"""
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.cost_micros, metrics.conversions,
               metrics.all_conversions_value,
               metrics.impressions, metrics.clicks,
               metrics.search_impression_share,
               metrics.search_budget_lost_impression_share
        FROM campaign
        WHERE segments.date DURING LAST_{lookback_days}_DAYS
        AND campaign.status = 'ENABLED'
    """


def campaign_performance_by_day_window(lookback_days: int) -> str:
    return f"""
        SELECT campaign.id, campaign.name, campaign.status,
               segments.date,
               metrics.cost_micros, metrics.conversions,
               metrics.all_conversions_value,
               metrics.impressions, metrics.clicks,
               metrics.search_impression_share,
               metrics.search_budget_lost_impression_share
        FROM campaign
        WHERE segments.date DURING LAST_{lookback_days}_DAYS
        AND campaign.status = 'ENABLED'
    """


def search_terms_window(lookback_days: int) -> str:
    return f"""
        SELECT search_term_view.search_term,
               campaign.id, campaign.name,
               ad_group.id,
               metrics.cost_micros, metrics.conversions,
               metrics.impressions, metrics.clicks
        FROM search_term_view
        WHERE segments.date DURING LAST_{lookback_days}_DAYS
        AND metrics.cost_micros > 0
    """


def _safe_prior_year(day: date) -> date:
    try:
        return day.replace(year=day.year - 1)
    except ValueError:
        return day.replace(month=2, day=28, year=day.year - 1)


def campaign_performance_same_period_last_year(lookback_days: int) -> str:
    today = date.today()
    current_end = today - timedelta(days=1)
    current_start = today - timedelta(days=lookback_days)
    prior_year_start = _safe_prior_year(current_start)
    prior_year_end = _safe_prior_year(current_end)
    return f"""
        SELECT campaign.id, campaign.name,
               metrics.cost_micros, metrics.conversions,
               metrics.all_conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{prior_year_start.isoformat()}' AND '{prior_year_end.isoformat()}'
        AND campaign.status = 'ENABLED'
    """


class QueryExecutor:
    def __init__(self, cache: DynamoDBCache | None = None) -> None:
        self.cache = cache or DynamoDBCache()
        self.last_cache_hit = False

    def _ttl_seconds_for_query(self, query: str) -> int:
        normalized = " ".join(query.upper().split())
        if " TODAY " in f" {normalized} " or " LAST_1_DAYS" in normalized:
            return 300
        return 1800

    def _to_plain_dict(self, row: Any) -> dict[str, Any]:
        if isinstance(row, dict):
            return {str(key): self._to_plain_value(value) for key, value in row.items()}
        if MessageToDict is not None and hasattr(row, "_pb"):
            return MessageToDict(row._pb, preserving_proto_field_name=True)
        if hasattr(row, "__dict__") and row.__dict__:
            return {str(key): self._to_plain_value(value) for key, value in row.__dict__.items() if not key.startswith("_")}
        return {"value": self._to_plain_value(row)}

    def _to_plain_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): self._to_plain_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._to_plain_value(item) for item in value]
        if MessageToDict is not None and hasattr(value, "_pb"):
            return MessageToDict(value._pb, preserving_proto_field_name=True)
        if hasattr(value, "__dict__") and value.__dict__:
            return {str(key): self._to_plain_value(item) for key, item in value.__dict__.items() if not key.startswith("_")}
        return value

    def _convert_micros_fields(self, payload: Any) -> Any:
        if isinstance(payload, list):
            return [self._convert_micros_fields(item) for item in payload]
        if not isinstance(payload, dict):
            return payload

        converted: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                converted[key] = self._convert_micros_fields(value)
                continue

            snake_match = re.fullmatch(r"(.+)_micros", key)
            camel_match = re.fullmatch(r"(.+)Micros", key)
            if snake_match:
                converted[snake_match.group(1)] = float(value) / 1_000_000
                continue
            if camel_match:
                stripped = camel_match.group(1)
                converted[stripped[0].lower() + stripped[1:]] = float(value) / 1_000_000
                continue
            converted[key] = value
        return converted

    def run(self, client: Any, customer_id: str, query: str) -> list[dict[str, Any]]:
        cached = self.cache.get(customer_id=customer_id, query=query)
        self.last_cache_hit = cached.hit
        if cached.hit:
            return cached.value

        service = client.get_service("GoogleAdsService")
        response = service.search(customer_id=customer_id, query=query)
        rows = list(response) if isinstance(response, Iterable) else []
        plain_rows = [self._convert_micros_fields(self._to_plain_dict(row)) for row in rows]
        self.cache.put(customer_id=customer_id, query=query, payload=plain_rows, ttl_seconds=self._ttl_seconds_for_query(query))
        return plain_rows
