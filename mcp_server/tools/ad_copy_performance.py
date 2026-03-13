from __future__ import annotations

from pydantic import BaseModel


class AdCopyPerformanceResult(BaseModel):
    status: str
    message: str


def ad_copy_performance(customer_id: str, vertical: str) -> AdCopyPerformanceResult:
    return AdCopyPerformanceResult(
        status="stub",
        message=f"Ad copy performance scaffold created for customer {customer_id} / vertical {vertical}.",
    )
