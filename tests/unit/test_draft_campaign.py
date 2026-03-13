from __future__ import annotations

from mcp_server.tools.draft_campaign import draft_campaign


def test_draft_campaign_always_requires_human_review() -> None:
    result = draft_campaign(
        customer_id="123",
        vertical="day_spa",
        campaign_goal="bookings",
        monthly_budget=3000.0,
        target_geography="Toronto",
    )
    assert result.requires_human_review is True
    assert result.ad_groups
