from __future__ import annotations

from mcp_server.cache.dynamodb_cache import DynamoDBCache
from mcp_server.tools.search_terms_audit import search_terms_audit
from tests.conftest import MappingQueryExecutor


def test_search_terms_audit_uses_classification_cache(dummy_auth) -> None:
    calls = {"count": 0}

    def classifier(vertical: str, term: str):  # noqa: ARG001
        calls["count"] += 1
        return {
            "classification": "irrelevant_zero_conversion",
            "recommended_negative_match_type": "phrase",
            "reason": "Mocked irrelevant term.",
            "confidence": 0.9,
        }

    query_executor = MappingQueryExecutor(
        {
            "FROM search_term_view": [
                {
                    "search_term_view": {"search_term": "free spa jobs"},
                    "metrics": {"cost": 25.0, "conversions": 0.0, "impressions": 100.0, "clicks": 15.0},
                }
            ]
        }
    )
    cache = DynamoDBCache(table_name=None)
    first = search_terms_audit("123", "day_spa", query_executor=query_executor, auth=dummy_auth, classifier=classifier, cache=cache)
    second = search_terms_audit("123", "day_spa", query_executor=query_executor, auth=dummy_auth, classifier=classifier, cache=cache)
    assert first.recommended_negatives_by_match_type["phrase"] == ["free spa jobs"]
    assert second.recommended_negatives_by_match_type["phrase"] == ["free spa jobs"]
    assert calls["count"] == 1
