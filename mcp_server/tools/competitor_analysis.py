from __future__ import annotations

from pydantic import BaseModel


class CompetitorAnalysisResult(BaseModel):
    status: str
    message: str


def competitor_analysis(customer_id: str, vertical: str) -> CompetitorAnalysisResult:
    return CompetitorAnalysisResult(
        status="stub",
        message=f"Competitor analysis scaffold created for customer {customer_id} / vertical {vertical}.",
    )
