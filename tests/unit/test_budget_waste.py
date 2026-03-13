from __future__ import annotations

from mcp_server.tools.analyze_budget_waste import analyze_budget_waste
from tests.conftest import MappingQueryExecutor


def test_budget_waste_detects_all_four_signals(dummy_auth) -> None:
    campaigns = [
        {
            "campaign": {"id": "1", "name": "Zero Conv"},
            "metrics": {"cost": 800.0, "conversions": 0.0, "all_conversions_value": 0.0, "search_budget_lost_impression_share": 0.1},
        },
        {
            "campaign": {"id": "2", "name": "Budget Starved Winner"},
            "metrics": {"cost": 400.0, "conversions": 8.0, "all_conversions_value": 1600.0, "search_budget_lost_impression_share": 0.35},
        },
    ]
    quality_rows = [
        {
            "campaign": {"id": "3", "name": "Low QS"},
            "ad_group_criterion": {"quality_info": {"quality_score": 3}},
            "metrics": {"cost": 200.0, "conversions": 1.0},
        }
    ]
    search_terms = [
        {
            "campaign": {"id": "4", "name": "Irrelevant Terms"},
            "search_term_view": {"search_term": "free spa jobs"},
            "metrics": {"cost": 50.0, "conversions": 0.0},
        }
    ]
    query_executor = MappingQueryExecutor(
        {
            "FROM campaign": campaigns,
            "FROM ad_group_criterion": quality_rows,
            "FROM search_term_view": search_terms,
        }
    )
    result = analyze_budget_waste("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    categories = {item.waste_category for item in result.wasted_campaigns}
    assert {"zero_conversion", "low_quality_score", "irrelevant_terms", "misallocated_budget"} <= categories
