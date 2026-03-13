from __future__ import annotations

from typing import Any, Literal
import json
import os

from pydantic import BaseModel

from mcp_server.auth.google_oauth import GoogleAdsAuth
from mcp_server.cache.dynamodb_cache import DynamoDBCache
from mcp_server.config.thresholds import NEGATIVE_KEYWORD_MIN_SPEND
from mcp_server.config.verticals import VERTICALS
from mcp_server.gaql.queries import QueryExecutor, SEARCH_TERMS_REPORT

try:
    from anthropic import Anthropic
except ImportError:  # pragma: no cover - dependency installed in target runtime
    Anthropic = None  # type: ignore[assignment]


TERM_CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "classification": {
            "type": "string",
            "enum": ["converting", "irrelevant_zero_conversion", "relevant_zero_conversion", "high_volume_low_quality"],
        },
        "recommended_negative_match_type": {"type": "string", "enum": ["exact", "phrase", "broad", "none"]},
        "reason": {"type": "string"},
        "confidence": {"type": "number"},
    },
    "required": ["classification", "recommended_negative_match_type", "reason", "confidence"],
}


class SearchTermFinding(BaseModel):
    search_term: str
    classification: Literal["converting", "irrelevant_zero_conversion", "relevant_zero_conversion", "high_volume_low_quality"]
    spend: float
    conversions: float
    impressions: float
    clicks: float
    recommended_negative_match_type: Literal["exact", "phrase", "broad", "none"]
    rationale: str


class SearchTermsAuditResult(BaseModel):
    terms: list[SearchTermFinding]
    recommended_negatives_by_match_type: dict[str, list[str]]
    estimated_monthly_spend_recovery: float


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


def _fallback_classification(vertical: str, term: str, spend: float, conversions: float, clicks: float) -> dict[str, Any]:
    suspicious_tokens = {"free", "jobs", "career", "auction", "cheap", "template", "diy"}
    lowered = term.lower()
    if conversions > 0:
        return {
            "classification": "converting",
            "recommended_negative_match_type": "none",
            "reason": f"Term converts for {vertical}.",
            "confidence": 0.95,
        }
    if any(token in lowered for token in suspicious_tokens):
        return {
            "classification": "irrelevant_zero_conversion",
            "recommended_negative_match_type": "phrase",
            "reason": "Contains low-intent or clearly irrelevant modifiers.",
            "confidence": 0.88,
        }
    if clicks >= 25 and spend >= NEGATIVE_KEYWORD_MIN_SPEND * 4:
        return {
            "classification": "high_volume_low_quality",
            "recommended_negative_match_type": "exact",
            "reason": "High-volume, non-converting search term likely wasting spend.",
            "confidence": 0.72,
        }
    return {
        "classification": "relevant_zero_conversion",
        "recommended_negative_match_type": "none",
        "reason": "Term is plausibly relevant but lacks conversion evidence.",
        "confidence": 0.63,
    }


def _classify_term_with_claude(vertical: str, term: str) -> dict[str, Any]:
    if Anthropic is None or not os.getenv("ANTHROPIC_API_KEY"):
        return _fallback_classification(vertical, term, spend=0.0, conversions=0.0, clicks=0.0)
    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    prompt = (
        "Classify this paid-search term for the given vertical. "
        "Return only valid JSON matching the provided schema.\n"
        f"Vertical: {vertical}\n"
        f"Search term: {term}\n"
        f"Schema: {json.dumps(TERM_CLASSIFICATION_SCHEMA)}"
    )
    response = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=250,
        messages=[{"role": "user", "content": prompt}],
    )
    text_blocks = [block.text for block in getattr(response, "content", []) if getattr(block, "type", "") == "text"]
    try:
        return json.loads("".join(text_blocks))
    except json.JSONDecodeError:
        return _fallback_classification(vertical, term, spend=0.0, conversions=0.0, clicks=0.0)


def _cached_term_classification(cache: DynamoDBCache, vertical: str, term: str, classifier: Any | None = None) -> dict[str, Any]:
    cache_key = f"term_classification:{vertical}:{term.lower()}"
    cached = cache.get(customer_id=vertical, query=cache_key)
    if cached.hit and cached.value:
        return cached.value[0]
    classifier = classifier or _classify_term_with_claude
    result = classifier(vertical, term)
    cache.put(customer_id=vertical, query=cache_key, payload=[result], ttl_seconds=30 * 24 * 3600)
    return result


def search_terms_audit(
    customer_id: str,
    vertical: str,
    min_spend: float = 5.0,
    query_executor: QueryExecutor | None = None,
    auth: GoogleAdsAuth | None = None,
    client: Any | None = None,
    classifier: Any | None = None,
    cache: DynamoDBCache | None = None,
) -> SearchTermsAuditResult:
    if vertical not in VERTICALS:
        raise ValueError(f"Unknown vertical '{vertical}'")

    query_executor = query_executor or QueryExecutor()
    auth = auth or GoogleAdsAuth()
    client = client or auth.get_client(customer_id)
    cache = cache or DynamoDBCache()
    rows = query_executor.run(client, customer_id, SEARCH_TERMS_REPORT)

    findings: list[SearchTermFinding] = []
    negatives = {"exact": [], "phrase": [], "broad": []}
    estimated_monthly_recovery = 0.0

    for row in rows:
        spend = _metric_value(row, "metrics", "cost", default=0.0)
        if spend < min_spend:
            continue
        term = str(row.get("search_term_view", {}).get("search_term", ""))
        conversions = _metric_value(row, "metrics", "conversions", default=0.0)
        impressions = _metric_value(row, "metrics", "impressions", default=0.0)
        clicks = _metric_value(row, "metrics", "clicks", default=0.0)
        classification = _cached_term_classification(cache, vertical, term, classifier=classifier)
        if conversions > 0:
            classification = {
                "classification": "converting",
                "recommended_negative_match_type": "none",
                "reason": "Observed conversions in current lookback period.",
                "confidence": 0.99,
            }

        finding = SearchTermFinding(
            search_term=term,
            classification=classification["classification"],
            spend=round(spend, 2),
            conversions=round(conversions, 2),
            impressions=round(impressions, 2),
            clicks=round(clicks, 2),
            recommended_negative_match_type=classification["recommended_negative_match_type"],
            rationale=classification["reason"],
        )
        findings.append(finding)
        match_type = finding.recommended_negative_match_type
        if match_type != "none" and finding.classification == "irrelevant_zero_conversion":
            negatives[match_type].append(term)
            estimated_monthly_recovery += spend

    return SearchTermsAuditResult(
        terms=findings,
        recommended_negatives_by_match_type=negatives,
        estimated_monthly_spend_recovery=round(estimated_monthly_recovery, 2),
    )
