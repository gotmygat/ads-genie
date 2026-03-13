from __future__ import annotations

import pytest

from mcp_server.tools.health_check import health_check
from tests.conftest import MappingQueryExecutor, make_performance_rows


def test_health_check_returns_healthy(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "FROM campaign": make_performance_rows(cost=100.0, conversions=10.0, value=400.0, lost_budget=0.1),
            "FROM ad_group_criterion": [
                {"ad_group_criterion": {"quality_info": {"quality_score": 7}}},
                {"ad_group_criterion": {"quality_info": {"quality_score": 8}}},
            ],
        }
    )
    result = health_check("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert result.overall_status == "healthy"


def test_health_check_roas_below_minimum_is_warning_or_worse(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "FROM campaign": make_performance_rows(cost=100.0, conversions=2.0, value=120.0, lost_budget=0.1),
            "FROM ad_group_criterion": [{"ad_group_criterion": {"quality_info": {"quality_score": 7}}}],
        }
    )
    result = health_check("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert result.overall_status in {"warning", "critical"}


def test_health_check_quality_score_adds_recommendation(dummy_auth) -> None:
    query_executor = MappingQueryExecutor(
        {
            "FROM campaign": make_performance_rows(cost=100.0, conversions=10.0, value=350.0, lost_budget=0.1),
            "FROM ad_group_criterion": [{"ad_group_criterion": {"quality_info": {"quality_score": 3}}}],
        }
    )
    result = health_check("123", "day_spa", query_executor=query_executor, auth=dummy_auth)
    assert any("quality score" in action.lower() for action in result.recommended_actions)


def test_health_check_invalid_vertical_raises() -> None:
    with pytest.raises(ValueError, match="Unknown vertical"):
        health_check("123", "not_real_vertical")
