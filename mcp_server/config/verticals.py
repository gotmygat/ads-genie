from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VerticalConfig:
    name: str
    min_acceptable_roas: float
    max_acceptable_cpa: float
    min_quality_score: int
    waste_cost_threshold_daily: float
    healthy_impression_share_min: float
    benchmark_ctr: float
    typical_search_terms_per_campaign: int
    notes: str


VERTICALS = {
    "day_spa": VerticalConfig(
        name="Day Spa",
        min_acceptable_roas=3.0,
        max_acceptable_cpa=45.0,
        min_quality_score=6,
        waste_cost_threshold_daily=15.0,
        healthy_impression_share_min=0.35,
        benchmark_ctr=0.08,
        typical_search_terms_per_campaign=40,
        notes="Appointment-driven. Conversion = booking form or call.",
    ),
    "storage_facility": VerticalConfig(
        name="Storage Facility",
        min_acceptable_roas=2.0,
        max_acceptable_cpa=75.0,
        min_quality_score=5,
        waste_cost_threshold_daily=25.0,
        healthy_impression_share_min=0.40,
        benchmark_ctr=0.06,
        typical_search_terms_per_campaign=30,
        notes="High CPA tolerance due to long customer LTV.",
    ),
    "dental": VerticalConfig(
        name="Dental",
        min_acceptable_roas=4.0,
        max_acceptable_cpa=180.0,
        min_quality_score=6,
        waste_cost_threshold_daily=40.0,
        healthy_impression_share_min=0.32,
        benchmark_ctr=0.07,
        typical_search_terms_per_campaign=35,
        notes="High-value lead gen with compliance-sensitive ad copy.",
    ),
    "med_spa": VerticalConfig(
        name="Med Spa",
        min_acceptable_roas=3.2,
        max_acceptable_cpa=65.0,
        min_quality_score=6,
        waste_cost_threshold_daily=20.0,
        healthy_impression_share_min=0.34,
        benchmark_ctr=0.075,
        typical_search_terms_per_campaign=42,
        notes="Offer sensitivity is high; brand tone matters materially to CVR.",
    ),
    "hvac": VerticalConfig(
        name="HVAC",
        min_acceptable_roas=5.0,
        max_acceptable_cpa=110.0,
        min_quality_score=6,
        waste_cost_threshold_daily=30.0,
        healthy_impression_share_min=0.38,
        benchmark_ctr=0.09,
        typical_search_terms_per_campaign=28,
        notes="Urgency-driven demand with strong local intent and mobile calls.",
    ),
    "plumbing": VerticalConfig(
        name="Plumbing",
        min_acceptable_roas=4.5,
        max_acceptable_cpa=95.0,
        min_quality_score=6,
        waste_cost_threshold_daily=28.0,
        healthy_impression_share_min=0.37,
        benchmark_ctr=0.085,
        typical_search_terms_per_campaign=30,
        notes="Emergency service modifiers heavily affect auction behavior.",
    ),
    "roofing": VerticalConfig(
        name="Roofing",
        min_acceptable_roas=6.0,
        max_acceptable_cpa=180.0,
        min_quality_score=5,
        waste_cost_threshold_daily=45.0,
        healthy_impression_share_min=0.33,
        benchmark_ctr=0.055,
        typical_search_terms_per_campaign=22,
        notes="Long sales cycle; lead quality is more important than raw lead count.",
    ),
    "legal": VerticalConfig(
        name="Legal",
        min_acceptable_roas=3.5,
        max_acceptable_cpa=250.0,
        min_quality_score=5,
        waste_cost_threshold_daily=60.0,
        healthy_impression_share_min=0.29,
        benchmark_ctr=0.06,
        typical_search_terms_per_campaign=26,
        notes="High CPC vertical where irrelevant terms can become expensive quickly.",
    ),
    "landscaping": VerticalConfig(
        name="Landscaping",
        min_acceptable_roas=3.2,
        max_acceptable_cpa=85.0,
        min_quality_score=6,
        waste_cost_threshold_daily=22.0,
        healthy_impression_share_min=0.36,
        benchmark_ctr=0.07,
        typical_search_terms_per_campaign=32,
        notes="Seasonality is strong; service mix varies by month and geography.",
    ),
    "pest_control": VerticalConfig(
        name="Pest Control",
        min_acceptable_roas=4.0,
        max_acceptable_cpa=70.0,
        min_quality_score=6,
        waste_cost_threshold_daily=18.0,
        healthy_impression_share_min=0.39,
        benchmark_ctr=0.082,
        typical_search_terms_per_campaign=27,
        notes="Fast response time and local coverage strongly influence conversion rate.",
    ),
    "auto_detailing": VerticalConfig(
        name="Auto Detailing",
        min_acceptable_roas=2.8,
        max_acceptable_cpa=40.0,
        min_quality_score=6,
        waste_cost_threshold_daily=12.0,
        healthy_impression_share_min=0.35,
        benchmark_ctr=0.078,
        typical_search_terms_per_campaign=34,
        notes="Service bundling and gift-card modifiers often affect performance.",
    ),
    "chiropractic": VerticalConfig(
        name="Chiropractic",
        min_acceptable_roas=3.6,
        max_acceptable_cpa=55.0,
        min_quality_score=6,
        waste_cost_threshold_daily=18.0,
        healthy_impression_share_min=0.31,
        benchmark_ctr=0.074,
        typical_search_terms_per_campaign=33,
        notes="Pain-intent keywords convert differently than wellness-intent keywords.",
    ),
}
