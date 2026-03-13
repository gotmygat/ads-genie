from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any
import logging
import os

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]

try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import JSONResponse
except ImportError:  # pragma: no cover - dependency installed in target runtime
    FastAPI = None  # type: ignore[assignment]
    HTTPException = RuntimeError  # type: ignore[assignment]
    Request = object  # type: ignore[assignment]
    JSONResponse = dict  # type: ignore[assignment]

try:
    from mangum import Mangum
except ImportError:  # pragma: no cover - optional for Lambda deployment
    Mangum = None  # type: ignore[assignment]

from pydantic import BaseModel, Field

try:
    import structlog
except ImportError:  # pragma: no cover - fallback for bare environments
    structlog = None

from mcp_server.auth.google_oauth import GoogleAdsAuth, SecretConfigurationError
from mcp_server.cache.dynamodb_cache import DynamoDBCache
from mcp_server.gaql.queries import QueryExecutor
from mcp_server.tools.ad_copy_performance import ad_copy_performance
from mcp_server.tools.analyze_budget_waste import analyze_budget_waste
from mcp_server.tools.benchmark_account import benchmark_account
from mcp_server.tools.competitor_analysis import competitor_analysis
from mcp_server.tools.cross_mcc_anomalies import cross_mcc_anomalies
from mcp_server.tools.diagnose_roas_drop import diagnose_roas_drop
from mcp_server.tools.draft_campaign import draft_campaign
from mcp_server.tools.generate_negative_keywords import generate_negative_keywords
from mcp_server.tools.health_check import health_check
from mcp_server.tools.keyword_expansion import keyword_expansion
from mcp_server.tools.search_terms_audit import search_terms_audit


LOGGER = structlog.get_logger(__name__) if structlog else logging.getLogger(__name__)


class ToolInvocationRequest(BaseModel):
    customer_id: str | None = None
    vertical: str | None = None
    tool_input: dict[str, Any] = Field(default_factory=dict)


def validate_outbound_response(tool_name: str, payload: Any) -> None:
    if tool_name == "draft_campaign" and getattr(payload, "requires_human_review", True) is False:
        raise HTTPException(status_code=500, detail="POLICY_VIOLATION: draft_campaign must require human review.")


def _dynamodb_health() -> dict[str, Any]:
    table_names = [
        os.getenv("DYNAMODB_ACCOUNTS_TABLE", "").strip(),
        os.getenv("DYNAMODB_DECISIONS_TABLE", "").strip(),
        os.getenv("DYNAMODB_AUTONOMY_TABLE", "").strip(),
        os.getenv("DYNAMODB_CACHE_TABLE", "").strip(),
    ]
    configured_tables = [name for name in table_names if name]
    if not configured_tables:
        return {"status": "not_configured"}
    if boto3 is None:
        return {"status": "dependency_missing", "dependency": "boto3"}

    dynamodb = boto3.client("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1"))
    discovered: list[str] = []
    for table_name in configured_tables:
        try:
            dynamodb.describe_table(TableName=table_name)
            discovered.append(table_name)
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            return {"status": "error", "error": str(exc), "table": table_name}
    return {"status": "ok", "tables": discovered}


def _anthropic_health() -> dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"status": "not_configured"}
    return {"status": "configured"}


def _google_ads_health() -> dict[str, Any]:
    try:
        auth = GoogleAdsAuth()
    except Exception as exc:  # pragma: no cover - config dependent
        return {"status": "error", "error": str(exc)}

    credentials = auth._credentials  # noqa: SLF001
    if not all(
        [
            credentials.client_id,
            credentials.client_secret,
            credentials.developer_token,
            credentials.refresh_token,
            credentials.mcc_customer_id,
        ]
    ):
        return {"status": "not_configured"}

    try:
        accounts = auth.list_accessible_accounts()
        return {"status": "ok", "accessible_account_count": len(accounts)}
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        return {"status": "error", "error": str(exc)}


def create_app() -> FastAPI:
    if FastAPI is None:
        raise RuntimeError("fastapi dependency is required to run the MCP server")
    if os.getenv("ENV", "local").strip().lower() == "production":
        GoogleAdsAuth()

    app = FastAPI(title="Ads Genie MCP Server", version="1.0.0")

    @app.middleware("http")
    async def ai_firewall(request: Request, call_next):  # type: ignore[override]
        if request.method == "POST" and request.url.path.startswith("/tools/"):
            try:
                body = await request.json()
                ToolInvocationRequest.model_validate(body)
            except Exception as exc:
                return JSONResponse(status_code=400, content={"detail": f"Inbound validation failed: {exc}"})
        response = await call_next(request)
        return response

    @app.exception_handler(SecretConfigurationError)
    async def configuration_exception_handler(_request: Request, exc: SecretConfigurationError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependencies": {
                "google_ads": _google_ads_health(),
                "dynamodb": _dynamodb_health(),
                "anthropic": _anthropic_health(),
            },
        }

    tool_registry = {
        "health_check": health_check,
        "analyze_budget_waste": analyze_budget_waste,
        "diagnose_roas_drop": diagnose_roas_drop,
        "search_terms_audit": search_terms_audit,
        "benchmark_account": benchmark_account,
        "generate_negative_keywords": generate_negative_keywords,
        "cross_mcc_anomalies": cross_mcc_anomalies,
        "draft_campaign": draft_campaign,
        "competitor_analysis": competitor_analysis,
        "keyword_expansion": keyword_expansion,
        "ad_copy_performance": ad_copy_performance,
    }

    @app.post("/tools/{tool_name}")
    async def invoke_tool(tool_name: str, payload: ToolInvocationRequest) -> Any:
        if tool_name not in tool_registry:
            raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")

        started = perf_counter()
        query_executor = QueryExecutor(cache=DynamoDBCache())
        auth = GoogleAdsAuth()
        tool = tool_registry[tool_name]
        try:
            if tool_name == "cross_mcc_anomalies":
                result = tool(vertical=payload.vertical)
            elif tool_name == "draft_campaign":
                if not payload.customer_id or not payload.vertical:
                    raise HTTPException(status_code=400, detail="customer_id and vertical are required")
                result = tool(
                    customer_id=payload.customer_id,
                    vertical=payload.vertical,
                    campaign_goal=str(payload.tool_input.get("campaign_goal", "")),
                    monthly_budget=float(payload.tool_input.get("monthly_budget", 0.0)),
                    target_geography=str(payload.tool_input.get("target_geography", "")),
                )
            else:
                if not payload.customer_id or not payload.vertical:
                    raise HTTPException(status_code=400, detail="customer_id and vertical are required")
                result = tool(
                    customer_id=payload.customer_id,
                    vertical=payload.vertical,
                    query_executor=query_executor,
                    auth=auth,
                )
            validate_outbound_response(tool_name, result)
            duration_ms = round((perf_counter() - started) * 1000, 2)
            LOGGER.info(
                "tool_invocation",
                tool_name=tool_name,
                customer_id=payload.customer_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                cache_hit=query_executor.last_cache_hit,
                response_status="success",
            )
            return result
        except HTTPException:
            raise
        except Exception as exc:
            duration_ms = round((perf_counter() - started) * 1000, 2)
            LOGGER.exception(
                "tool_invocation_failed",
                tool_name=tool_name,
                customer_id=payload.customer_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                cache_hit=query_executor.last_cache_hit,
                response_status="error",
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return app


app = create_app() if FastAPI is not None else None
handler = Mangum(app) if app is not None and Mangum is not None else None
