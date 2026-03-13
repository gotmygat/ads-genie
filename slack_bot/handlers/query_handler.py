from __future__ import annotations

from typing import Any, Callable
import json
import os

try:
    from anthropic import Anthropic
except ImportError:  # pragma: no cover
    Anthropic = None  # type: ignore[assignment]

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.gaql.queries import QueryExecutor
from mcp_server.tools.analyze_budget_waste import analyze_budget_waste
from mcp_server.tools.benchmark_account import benchmark_account
from mcp_server.tools.cross_mcc_anomalies import cross_mcc_anomalies
from mcp_server.tools.diagnose_roas_drop import diagnose_roas_drop
from mcp_server.tools.generate_negative_keywords import generate_negative_keywords
from mcp_server.tools.health_check import health_check
from orchestration.models.account_registry import AccountRegistry


TOOL_MAP: dict[str, Callable[..., Any]] = {
    "health_check": health_check,
    "analyze_budget_waste": analyze_budget_waste,
    "diagnose_roas_drop": diagnose_roas_drop,
    "generate_negative_keywords": generate_negative_keywords,
    "benchmark_account": benchmark_account,
    "cross_mcc_anomalies": cross_mcc_anomalies,
}

KEYWORD_INTENTS = {
    "health_check": ["how are we doing", "health check", "health", "performance"],
    "analyze_budget_waste": ["wasting money", "budget waste", "waste"],
    "diagnose_roas_drop": ["why did roas drop", "roas drop", "roas fell"],
    "generate_negative_keywords": ["negative keywords", "what negatives", "add negatives"],
    "benchmark_account": ["compare", "benchmark", "how does this account compare"],
    "cross_mcc_anomalies": ["across all accounts", "all accounts", "what's wrong across all accounts"],
}


def _parse_intent_with_keywords(text: str) -> str:
    normalized = text.lower()
    for intent, phrases in KEYWORD_INTENTS.items():
        if any(phrase in normalized for phrase in phrases):
            return intent
    return "health_check"


def parse_intent(text: str) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key or Anthropic is None:
        return _parse_intent_with_keywords(text)
    try:
        client = Anthropic(api_key=api_key)
        prompt = (
            "Classify the Slack query into one of these intents only: "
            "health_check, analyze_budget_waste, diagnose_roas_drop, generate_negative_keywords, "
            "benchmark_account, cross_mcc_anomalies. Reply with JSON: {\"intent\": \"...\"}.\n\n"
            f"Message: {text}"
        )
        response = client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=128,
            messages=[{"role": "user", "content": prompt}],
        )
        content = "".join(block.text for block in response.content if getattr(block, "type", "") == "text")
        payload = json.loads(content)
        return str(payload.get("intent", "health_check"))
    except Exception:
        return _parse_intent_with_keywords(text)


def resolve_customer_from_channel(channel_id: str, registry: AccountRegistry | None = None) -> dict[str, Any] | None:
    registry = registry or AccountRegistry()
    for account in registry.list_active_accounts():
        if str(account.get("slack_channel_id", "")) == str(channel_id):
            return account
    return None


def handle_query_message(
    *,
    channel_id: str,
    text: str,
    registry: AccountRegistry | None = None,
    auth: GoogleAdsAuth | None = None,
    query_executor: QueryExecutor | None = None,
) -> dict[str, Any]:
    registry = registry or AccountRegistry()
    intent = parse_intent(text)
    if intent != "cross_mcc_anomalies":
        account = resolve_customer_from_channel(channel_id, registry=registry)
        if account is None:
            return {
                "text": "This channel isn't linked to a Google Ads account. Please ask your admin to configure channel mapping.",
            }
        auth = auth or GoogleAdsAuth()
        query_executor = query_executor or QueryExecutor()
        tool = TOOL_MAP[intent]
        result = tool(
            customer_id=str(account["customer_id"]),
            vertical=str(account["vertical"]),
            auth=auth,
            query_executor=query_executor,
        )
        return {
            "text": f"{intent} for {account['account_name']}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{intent}*\n```{json.dumps(result.model_dump(mode='json'), indent=2)}```",
                    },
                }
            ],
        }

    result = cross_mcc_anomalies()
    return {
        "text": "cross_mcc_anomalies",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*cross_mcc_anomalies*\n```{json.dumps(result.model_dump(mode='json'), indent=2)}```",
                },
            }
        ],
    }
