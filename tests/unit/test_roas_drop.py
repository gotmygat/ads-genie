from __future__ import annotations

from mcp_server.tools.diagnose_roas_drop import diagnose_roas_drop
from tests.conftest import MappingQueryExecutor, make_daily_performance_rows


def test_roas_drop_prefers_search_terms_first(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "segments.date,": make_daily_performance_rows(current_value_ratio=2.0, prior_value_ratio=4.0),
            "FROM search_term_view": [{"metrics": {"cost": 150.0, "conversions": 0.0}}],
            "FROM auction_insight_campaign_date_range": [],
            "FROM ad_group_criterion": [],
        }
    )
    result = diagnose_roas_drop("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert result.primary_cause == "irrelevant_search_terms"


def test_roas_drop_detects_competitor_entry(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "segments.date,": make_daily_performance_rows(current_value_ratio=2.0, prior_value_ratio=4.0),
            "FROM search_term_view": [],
            "FROM auction_insight_campaign_date_range": [{"auction_insight": {"domain": "newcompetitor.com", "impression_share": 0.2}}],
            "FROM ad_group_criterion": [],
        }
    )
    result = diagnose_roas_drop("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert result.primary_cause == "competitor_entry"


def test_roas_drop_detects_quality_score_issue(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "segments.date,": make_daily_performance_rows(current_value_ratio=2.0, prior_value_ratio=4.0),
            "FROM search_term_view": [],
            "FROM auction_insight_campaign_date_range": [],
            "FROM ad_group_criterion": [{"ad_group_criterion": {"quality_info": {"quality_score": 4}}}],
        }
    )
    result = diagnose_roas_drop("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert result.primary_cause == "quality_score_drop"
