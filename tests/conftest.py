from __future__ import annotations

from datetime import date, timedelta

import pytest


class DummyAuth:
    def get_client(self, customer_id: str) -> object:
        return {"customer_id": customer_id}


class MappingQueryExecutor:
    def __init__(self, mapping: dict[str, list[dict]]) -> None:
        self.mapping = mapping

    def run(self, client, customer_id: str, query: str):  # noqa: ANN001
        for needle, payload in sorted(self.mapping.items(), key=lambda item: len(item[0]), reverse=True):
            if needle in query:
                return payload
        return []


def make_performance_rows(
    *,
    cost: float,
    conversions: float,
    value: float,
    lost_budget: float = 0.1,
    clicks: float = 100.0,
    impressions: float = 1000.0,
    name: str = "Campaign A",
    campaign_id: str = "111",
) -> list[dict]:
    return [
        {
            "campaign": {"id": campaign_id, "name": name, "status": "ENABLED"},
            "metrics": {
                "cost": cost,
                "conversions": conversions,
                "all_conversions_value": value,
                "search_budget_lost_impression_share": lost_budget,
                "clicks": clicks,
                "impressions": impressions,
            },
        }
    ]


def make_daily_performance_rows(
    current_value_ratio: float = 3.0,
    prior_value_ratio: float = 4.0,
    current_conversions: float = 10.0,
    prior_conversions: float = 10.0,
) -> list[dict]:
    rows: list[dict] = []
    today = date.today()
    for day_offset in range(1, 15):
        day = today - timedelta(days=day_offset)
        ratio = current_value_ratio if day_offset <= 7 else prior_value_ratio
        conversions = current_conversions if day_offset <= 7 else prior_conversions
        rows.append(
            {
                "campaign": {"id": "111", "name": "Campaign A", "status": "ENABLED"},
                "segments": {"date": day.isoformat()},
                "metrics": {
                    "cost": 100.0,
                    "conversions": conversions,
                    "all_conversions_value": 100.0 * ratio,
                    "clicks": 120.0,
                    "impressions": 1000.0,
                    "search_budget_lost_impression_share": 0.1,
                },
            }
        )
    return rows


@pytest.fixture
def dummy_auth() -> DummyAuth:
    return DummyAuth()
