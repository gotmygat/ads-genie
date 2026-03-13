from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from mcp_server.config.verticals import VERTICALS


class AdGroupDraft(BaseModel):
    name: str
    keywords: list[str]
    headlines: list[str]
    descriptions: list[str]


class CampaignDraft(BaseModel):
    campaign_name: str
    campaign_type: str
    bid_strategy: str
    daily_budget: float
    ad_groups: list[AdGroupDraft]
    account_level_negatives: list[str]
    estimated_monthly_clicks_range: tuple[int, int]
    estimated_monthly_cost_range: tuple[float, float]
    methodology_notes: str
    requires_human_review: bool


def draft_campaign(
    customer_id: str,
    vertical: str,
    campaign_goal: str,
    monthly_budget: float,
    target_geography: str,
) -> CampaignDraft:
    if vertical not in VERTICALS:
        raise ValueError(f"Unknown vertical '{vertical}'")

    theme_map = {
        "day_spa": ["facials", "massage", "gift cards", "membership"],
        "storage_facility": ["climate controlled", "vehicle storage", "self storage near me", "unit sizes"],
        "dental": ["implants", "emergency", "cosmetic", "invisalign"],
    }
    themes = theme_map.get(vertical, ["core service", "high intent", "brand", "competitor"])
    daily_budget = round(monthly_budget / 30, 2)
    ad_groups: list[AdGroupDraft] = []
    for theme in themes:
        ad_groups.append(
            AdGroupDraft(
                name=f"{theme.title()} STAG",
                keywords=[f"{theme} {target_geography}", f"{theme} near me", f"best {theme}"],
                headlines=[
                    f"{theme.title()} in {target_geography}",
                    f"Book {theme.title()} Today",
                    f"Top Rated {theme.title()}",
                ],
                descriptions=[
                    f"Goal: {campaign_goal}. Local coverage for {target_geography}.",
                    "Built with Render STAG methodology and human review required before launch.",
                ],
            )
        )

    return CampaignDraft(
        campaign_name=f"{VERTICALS[vertical].name} | {campaign_goal} | {target_geography}",
        campaign_type="search",
        bid_strategy="maximize_conversions",
        daily_budget=daily_budget,
        ad_groups=ad_groups,
        account_level_negatives=["free", "jobs", "career"],
        estimated_monthly_clicks_range=(int(monthly_budget * 1.5), int(monthly_budget * 2.5)),
        estimated_monthly_cost_range=(round(monthly_budget * 0.85, 2), round(monthly_budget * 1.1, 2)),
        methodology_notes="Single Theme Ad Groups with vertical-specific defaults and mandatory human review.",
        requires_human_review=True,
    )
