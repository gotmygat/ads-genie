from __future__ import annotations

from mcp_server.tools.generate_negative_keywords import generate_negative_keywords
from mcp_server.tools.search_terms_audit import SearchTermFinding, SearchTermsAuditResult


def test_generate_negative_keywords_combines_three_sources() -> None:
    audit = SearchTermsAuditResult(
        terms=[
            SearchTermFinding(
                search_term="free spa jobs",
                classification="irrelevant_zero_conversion",
                spend=25.0,
                conversions=0.0,
                impressions=100.0,
                clicks=15.0,
                recommended_negative_match_type="phrase",
                rationale="Mocked",
            )
        ],
        recommended_negatives_by_match_type={"exact": [], "phrase": ["free spa jobs"], "broad": []},
        estimated_monthly_spend_recovery=25.0,
    )
    result = generate_negative_keywords(
        "123",
        "day_spa",
        search_terms_audit_result=audit,
        mcc_negative_loader=lambda _vertical: [{"term": "spa training", "match_type": "phrase"}],
    )
    sources = {item.source for item in result.recommendations}
    assert {"this_account", "mcc_portfolio", "llm_generated"} <= sources
