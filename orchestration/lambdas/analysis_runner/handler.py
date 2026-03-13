from __future__ import annotations

from typing import Any
import logging

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.gaql.queries import QueryExecutor
from mcp_server.tools.analyze_budget_waste import analyze_budget_waste
from mcp_server.tools.diagnose_roas_drop import diagnose_roas_drop
from mcp_server.tools.health_check import health_check


LOGGER = logging.getLogger(__name__)


def _select_tool(payload: dict[str, Any]):
    health = payload.get("health_check", {})
    if health.get("zero_conversion_campaigns"):
        return "analyze_budget_waste", analyze_budget_waste
    if str(health.get("roas_status", "")).lower() != "healthy":
        return "diagnose_roas_drop", diagnose_roas_drop
    return "health_check", health_check


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    tool_name, tool = _select_tool(event)
    auth = GoogleAdsAuth()
    query_executor = QueryExecutor()
    result = tool(
        customer_id=str(event["customer_id"]),
        vertical=str(event["vertical"]),
        auth=auth,
        query_executor=query_executor,
    )
    response = dict(event)
    response["analysis_tool"] = tool_name
    response["analysis_result"] = result.model_dump(mode="json")
    response["analysis_request_id"] = getattr(context, "aws_request_id", None)
    LOGGER.info(
        "analysis_runner_complete",
        extra={
            "customer_id": event.get("customer_id"),
            "tool_name": tool_name,
        },
    )
    return response
