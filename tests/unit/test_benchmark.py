from __future__ import annotations

from mcp_server.tools.benchmark_account import benchmark_account
from tests.conftest import MappingQueryExecutor, make_performance_rows


def test_benchmark_account_handles_insufficient_vertical_data(dummy_auth) -> None:
    query_executor = MappingQueryExecutor({"FROM campaign": make_performance_rows(cost=100.0, conversions=5.0, value=300.0)})
    result = benchmark_account(
        "123",
        "day_spa",
        query_executor=query_executor,
        auth=dummy_auth,
        benchmark_loader=lambda _vertical: {"account_count": 2, "avg_roas": 3.0, "avg_cpa": 20.0, "avg_ctr": 0.1},
    )
    assert any("Insufficient benchmark data" in item for item in result.improvement_opportunities)
