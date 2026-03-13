from __future__ import annotations

from typing import Any, Literal
import os

try:
    import boto3
    from boto3.dynamodb.conditions import Key
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]
    Key = None  # type: ignore[assignment]
from pydantic import BaseModel

from mcp_server.config.verticals import VERTICALS
from mcp_server.tools.search_terms_audit import SearchTermsAuditResult, search_terms_audit


class NegativeKeywordRecommendation(BaseModel):
    term: str
    match_type: str
    rationale: str
    source: Literal["this_account", "mcc_portfolio", "llm_generated"]
    confidence: float
    estimated_spend_recovery: float


class NegativeKeywordRecommendations(BaseModel):
    recommendations: list[NegativeKeywordRecommendation]


def _load_portfolio_negatives(vertical: str, loader: Any | None = None) -> list[dict[str, Any]]:
    if loader is not None:
        return loader(vertical)
    if boto3 is None or Key is None:
        return []
    table_name = os.getenv("DYNAMODB_MCC_NEGATIVES_TABLE", "ads_genie_mcc_negatives")
    table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1")).Table(table_name)
    response = table.query(KeyConditionExpression=Key("vertical").eq(vertical))
    return response.get("Items", [])


def _llm_generated_fallback(existing_terms: set[str], vertical: str) -> list[NegativeKeywordRecommendation]:
    candidates = [f"free {vertical}", f"{vertical} jobs", f"{vertical} training"]
    return [
        NegativeKeywordRecommendation(
            term=term,
            match_type="phrase",
            rationale="Pattern-based generated negative based on common low-intent modifiers.",
            source="llm_generated",
            confidence=0.55,
            estimated_spend_recovery=0.0,
        )
        for term in candidates
        if term not in existing_terms
    ]


def generate_negative_keywords(
    customer_id: str,
    vertical: str,
    scope: Literal["account", "campaign", "ad_group"] = "campaign",
    search_terms_audit_result: SearchTermsAuditResult | None = None,
    search_terms_audit_fn: Any | None = None,
    mcc_negative_loader: Any | None = None,
) -> NegativeKeywordRecommendations:
    if vertical not in VERTICALS:
        raise ValueError(f"Unknown vertical '{vertical}'")

    audit_fn = search_terms_audit_fn or search_terms_audit
    audit = search_terms_audit_result or audit_fn(customer_id=customer_id, vertical=vertical)
    recommendations: list[NegativeKeywordRecommendation] = []

    for finding in audit.terms:
        if finding.classification != "irrelevant_zero_conversion":
            continue
        recommendations.append(
            NegativeKeywordRecommendation(
                term=finding.search_term,
                match_type=finding.recommended_negative_match_type,
                rationale=f"Observed on this account with spend ${finding.spend:.2f} and zero conversions.",
                source="this_account",
                confidence=0.92,
                estimated_spend_recovery=finding.spend,
            )
        )

    seen_terms = {item.term for item in recommendations}
    portfolio_items = _load_portfolio_negatives(vertical, loader=mcc_negative_loader)
    for item in portfolio_items:
        term = str(item.get("term", "")).strip()
        if not term or term in seen_terms:
            continue
        recommendations.append(
            NegativeKeywordRecommendation(
                term=term,
                match_type=str(item.get("match_type", "phrase")),
                rationale=str(item.get("rationale", "Confirmed negative across same-vertical portfolio accounts.")),
                source="mcc_portfolio",
                confidence=float(item.get("confidence", 0.84)),
                estimated_spend_recovery=float(item.get("estimated_spend_recovery", 0.0)),
            )
        )
        seen_terms.add(term)

    for suggestion in _llm_generated_fallback(seen_terms, vertical):
        recommendations.append(suggestion)

    return NegativeKeywordRecommendations(recommendations=recommendations)
