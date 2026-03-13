from __future__ import annotations

from pydantic import BaseModel


class KeywordExpansionResult(BaseModel):
    status: str
    message: str


def keyword_expansion(customer_id: str, vertical: str) -> KeywordExpansionResult:
    return KeywordExpansionResult(
        status="stub",
        message=f"Keyword expansion scaffold created for customer {customer_id} / vertical {vertical}.",
    )
