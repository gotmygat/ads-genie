from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Any, Callable
import math

from .ads_client import GoogleAdsAdapter
from .db import Database


@dataclass(frozen=True)
class VerticalBenchmark:
    roas_healthy: float
    cpa_target: float
    quality_score_min: float


BENCHMARKS: dict[str, VerticalBenchmark] = {
    "self_storage": VerticalBenchmark(roas_healthy=2.6, cpa_target=85, quality_score_min=6.0),
    "day_spa": VerticalBenchmark(roas_healthy=2.0, cpa_target=60, quality_score_min=6.5),
    "dental": VerticalBenchmark(roas_healthy=4.0, cpa_target=180, quality_score_min=6.8),
}


def _safe_div(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-9:
        return 0.0
    return numerator / denominator


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _severity_from_score(score: float) -> str:
    if score >= 80:
        return "critical"
    if score >= 55:
        return "high"
    if score >= 30:
        return "medium"
    if score > 0:
        return "low"
    return "none"


class ToolEngine:
    def __init__(self, db: Database, ads_adapter: GoogleAdsAdapter) -> None:
        self.db = db
        self.ads_adapter = ads_adapter
        self._dispatch: dict[str, Callable[[int, dict[str, Any]], dict[str, Any]]] = {
            "health_check": self.health_check,
            "analyze_budget_waste": self.analyze_budget_waste,
            "diagnose_roas_drop": self.diagnose_roas_drop,
            "search_terms_audit": self.search_terms_audit,
            "benchmark_account": self.benchmark_account,
            "generate_negative_keywords": self.generate_negative_keywords,
            "draft_campaign": self.draft_campaign,
            "cross_mcc_anomalies": self.cross_mcc_anomalies,
            "competitor_analysis": self.competitor_analysis,
            "landing_page_audit": self.landing_page_audit,
            "keyword_expansion": self.keyword_expansion,
            "ad_copy_performance": self.ad_copy_performance,
            "pacing": self.pacing,
        }

    def list_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "health_check",
                "description": "Vertical-aware account health diagnostic based on ROAS, CPA, quality score, and budget pressure.",
                "requires_account": True,
            },
            {
                "name": "analyze_budget_waste",
                "description": "Composite waste analysis: zero-conversion spend, irrelevant terms, low quality score drag, and budget misallocation.",
                "requires_account": True,
            },
            {
                "name": "diagnose_roas_drop",
                "description": "ROAS drop diagnosis following Render playbook order: search terms -> quality -> budget pressure -> seasonality note.",
                "requires_account": True,
            },
            {
                "name": "search_terms_audit",
                "description": "Classifies search terms and suggests negative keywords using deterministic heuristics.",
                "requires_account": True,
            },
            {
                "name": "benchmark_account",
                "description": "Compares account performance to MCC-style vertical benchmark targets.",
                "requires_account": True,
            },
            {
                "name": "generate_negative_keywords",
                "description": "Creates negative keyword candidates from irrelevant search term spend.",
                "requires_account": True,
            },
            {
                "name": "draft_campaign",
                "description": "Builds STAG-style draft campaign structure with vertical defaults.",
                "requires_account": True,
            },
            {
                "name": "cross_mcc_anomalies",
                "description": "Flags accounts deviating from cross-account portfolio patterns.",
                "requires_account": False,
            },
            {
                "name": "competitor_analysis",
                "description": "Placeholder for auction insight-based competitor movement analysis.",
                "requires_account": True,
            },
            {
                "name": "landing_page_audit",
                "description": "Placeholder for landing page quality/conversion friction checks.",
                "requires_account": True,
            },
            {
                "name": "keyword_expansion",
                "description": "Placeholder for deterministic long-tail keyword expansion.",
                "requires_account": True,
            },
            {
                "name": "ad_copy_performance",
                "description": "Placeholder for ad-copy CTR/CVR performance decomposition.",
                "requires_account": True,
            },
            {
                "name": "pacing",
                "description": "Placeholder for budget pacing and month-end risk estimation.",
                "requires_account": True,
            },
        ]

    def run_tool(self, tool_name: str, account_id: int | None, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        fn = self._dispatch.get(tool_name)
        if not fn:
            raise ValueError(f"Unknown tool: {tool_name}")
        if tool_name != "cross_mcc_anomalies" and account_id is None:
            raise ValueError(f"Tool {tool_name} requires account_id")
        if tool_name == "cross_mcc_anomalies":
            return fn(account_id or 0, params)
        return fn(int(account_id or 0), params)

    def _account_metrics(self, account_id: int) -> dict[str, Any]:
        snapshot = self.ads_adapter.fetch_account_snapshot(account_id)
        campaigns = snapshot.campaigns
        search_terms = snapshot.search_terms
        vertical = snapshot.account["vertical"]
        benchmark = BENCHMARKS.get(vertical, VerticalBenchmark(2.5, 100, 6.0))

        spend_7d = sum(float(c["spend_7d"]) for c in campaigns)
        spend_prev_7d = sum(float(c["spend_prev_7d"]) for c in campaigns)
        conversions_7d = sum(float(c["conversions_7d"]) for c in campaigns)
        conversions_prev_7d = sum(float(c["conversions_prev_7d"]) for c in campaigns)
        revenue_7d = sum(float(c["revenue_7d"]) for c in campaigns)
        revenue_prev_7d = sum(float(c["revenue_prev_7d"]) for c in campaigns)
        quality_avg = mean(float(c["quality_score_avg"]) for c in campaigns) if campaigns else 0.0
        lost_budget_avg = mean(float(c["impression_share_lost_budget"]) for c in campaigns) if campaigns else 0.0

        roas_7d = _safe_div(revenue_7d, spend_7d)
        roas_prev_7d = _safe_div(revenue_prev_7d, spend_prev_7d)
        cpa_7d = _safe_div(spend_7d, conversions_7d)
        cpa_prev_7d = _safe_div(spend_prev_7d, conversions_prev_7d)
        roas_drop_pct = _safe_div(roas_prev_7d - roas_7d, max(roas_prev_7d, 0.01)) * 100.0

        return {
            "account": snapshot.account,
            "campaigns": campaigns,
            "search_terms": search_terms,
            "context_memory": snapshot.context_memory,
            "vertical": vertical,
            "benchmark": benchmark,
            "spend_7d": spend_7d,
            "spend_prev_7d": spend_prev_7d,
            "conversions_7d": conversions_7d,
            "conversions_prev_7d": conversions_prev_7d,
            "revenue_7d": revenue_7d,
            "revenue_prev_7d": revenue_prev_7d,
            "quality_avg": quality_avg,
            "lost_budget_avg": lost_budget_avg,
            "roas_7d": roas_7d,
            "roas_prev_7d": roas_prev_7d,
            "cpa_7d": cpa_7d,
            "cpa_prev_7d": cpa_prev_7d,
            "roas_drop_pct": roas_drop_pct,
        }

    def health_check(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        benchmark: VerticalBenchmark = m["benchmark"]

        roas_gap = max(0.0, benchmark.roas_healthy - m["roas_7d"])
        cpa_gap = max(0.0, m["cpa_7d"] - benchmark.cpa_target)
        quality_gap = max(0.0, benchmark.quality_score_min - m["quality_avg"])

        score = 0.0
        score += _clamp(_safe_div(roas_gap, benchmark.roas_healthy) * 45.0, 0.0, 45.0)
        score += _clamp(_safe_div(cpa_gap, benchmark.cpa_target) * 25.0, 0.0, 25.0)
        score += _clamp(_safe_div(quality_gap, benchmark.quality_score_min) * 20.0, 0.0, 20.0)
        score += _clamp(m["lost_budget_avg"] * 10.0, 0.0, 10.0)
        score = round(_clamp(score, 0.0, 100.0), 2)

        flags: list[str] = []
        if roas_gap > 0:
            flags.append("ROAS below benchmark")
        if cpa_gap > 0:
            flags.append("CPA above target")
        if quality_gap > 0:
            flags.append("Quality score below threshold")
        if m["lost_budget_avg"] > 0.25:
            flags.append("Budget pressure on converting inventory")

        return {
            "tool": "health_check",
            "account_id": account_id,
            "vertical": m["vertical"],
            "severity": _severity_from_score(score),
            "risk_score": score,
            "flags": flags,
            "metrics": {
                "spend_7d": round(m["spend_7d"], 2),
                "spend_prev_7d": round(m["spend_prev_7d"], 2),
                "revenue_7d": round(m["revenue_7d"], 2),
                "revenue_prev_7d": round(m["revenue_prev_7d"], 2),
                "conversions_7d": round(m["conversions_7d"], 2),
                "conversions_prev_7d": round(m["conversions_prev_7d"], 2),
                "roas_7d": round(m["roas_7d"], 3),
                "roas_prev_7d": round(m["roas_prev_7d"], 3),
                "cpa_7d": round(m["cpa_7d"], 2),
                "cpa_prev_7d": round(m["cpa_prev_7d"], 2),
                "quality_score_avg": round(m["quality_avg"], 2),
                "impression_share_lost_budget": round(m["lost_budget_avg"], 3),
            },
            "benchmark": {
                "roas_healthy": benchmark.roas_healthy,
                "cpa_target": benchmark.cpa_target,
                "quality_score_min": benchmark.quality_score_min,
            },
        }

    def analyze_budget_waste(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        campaigns = m["campaigns"]
        terms = m["search_terms"]

        zero_conversion_spend = sum(
            float(c["spend_7d"]) for c in campaigns if float(c["conversions_7d"]) <= 0.0
        )
        low_qs_spend = sum(
            float(c["spend_7d"]) for c in campaigns if float(c["quality_score_avg"]) < 5.0
        )
        irrelevant_term_spend = sum(
            float(t["spend_7d"])
            for t in terms
            if (str(t["relevance"]).lower() in {"irrelevant", "borderline"} and float(t["conversions_7d"]) <= 0.0)
        )
        misallocated_budget_value = 0.0
        misallocated_campaigns: list[str] = []
        for c in campaigns:
            if float(c["impression_share_lost_budget"]) > 0.25 and float(c["conversions_7d"]) >= 5:
                misallocated_campaigns.append(str(c["name"]))
                misallocated_budget_value += float(c["spend_7d"]) * float(c["impression_share_lost_budget"])

        total_waste = zero_conversion_spend + irrelevant_term_spend + low_qs_spend + misallocated_budget_value
        waste_ratio = _safe_div(total_waste, max(m["spend_7d"], 0.01))
        waste_score = round(_clamp(waste_ratio * 100.0, 0.0, 100.0), 2)

        top_irrelevant_terms = [
            {
                "term": t["term"],
                "spend_7d": round(float(t["spend_7d"]), 2),
                "conversions_7d": float(t["conversions_7d"]),
            }
            for t in terms
            if str(t["relevance"]).lower() in {"irrelevant", "borderline"}
        ][:5]

        return {
            "tool": "analyze_budget_waste",
            "account_id": account_id,
            "severity": _severity_from_score(waste_score),
            "waste_score": waste_score,
            "components": {
                "zero_conversion_spend": round(zero_conversion_spend, 2),
                "irrelevant_term_spend": round(irrelevant_term_spend, 2),
                "low_quality_score_spend": round(low_qs_spend, 2),
                "misallocated_budget": round(misallocated_budget_value, 2),
                "estimated_total_waste": round(total_waste, 2),
                "waste_ratio": round(waste_ratio, 3),
            },
            "misallocated_campaigns": misallocated_campaigns,
            "top_irrelevant_terms": top_irrelevant_terms,
        }

    def diagnose_roas_drop(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        terms = m["search_terms"]

        roas_drop_pct = max(0.0, m["roas_drop_pct"])
        roas_drop_score = _clamp(roas_drop_pct, 0.0, 100.0)

        irrelevant_term_cost = sum(
            float(t["spend_7d"])
            for t in terms
            if str(t["relevance"]).lower() == "irrelevant"
        )
        root_causes: list[dict[str, Any]] = []

        if irrelevant_term_cost > 120:
            root_causes.append(
                {
                    "cause": "irrelevant_search_terms",
                    "evidence": f"${irrelevant_term_cost:.2f} spent on low-intent terms in last 7 days",
                }
            )
        if m["quality_avg"] < m["benchmark"].quality_score_min:
            root_causes.append(
                {
                    "cause": "quality_score_pressure",
                    "evidence": f"avg quality score {m['quality_avg']:.2f} vs target {m['benchmark'].quality_score_min:.1f}",
                }
            )
        if m["lost_budget_avg"] > 0.25:
            root_causes.append(
                {
                    "cause": "budget_constraint",
                    "evidence": f"{m['lost_budget_avg']*100:.1f}% impression share lost to budget",
                }
            )

        seasonality_note = next(
            (item["memory_value"] for item in m["context_memory"] if item["memory_key"] == "seasonality_note"),
            "",
        )
        if seasonality_note:
            root_causes.append({"cause": "context_memory", "evidence": seasonality_note})

        if not root_causes:
            root_causes.append(
                {
                    "cause": "no_single_smoking_gun",
                    "evidence": "ROAS drift detected with distributed minor factors; keep monitoring",
                }
            )

        return {
            "tool": "diagnose_roas_drop",
            "account_id": account_id,
            "severity": _severity_from_score(roas_drop_score),
            "roas": {
                "current_7d": round(m["roas_7d"], 3),
                "previous_7d": round(m["roas_prev_7d"], 3),
                "drop_pct": round(roas_drop_pct, 2),
            },
            "root_causes": root_causes,
        }

    def search_terms_audit(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        terms = m["search_terms"]
        suspicious_tokens = {
            "free",
            "jobs",
            "career",
            "auction",
            "cheap",
            "template",
            "diy",
        }

        audited: list[dict[str, Any]] = []
        negatives: list[str] = []
        for term in terms:
            label = str(term["relevance"]).lower().strip()
            text = str(term["term"]).lower()
            if label == "unknown":
                if any(token in text for token in suspicious_tokens):
                    label = "irrelevant"
                else:
                    label = "relevant"

            row = {
                "term": term["term"],
                "spend_7d": round(float(term["spend_7d"]), 2),
                "conversions_7d": float(term["conversions_7d"]),
                "quality_score": float(term["quality_score"]),
                "classification": label,
            }
            audited.append(row)

            if label == "irrelevant" and float(term["spend_7d"]) >= 40 and float(term["conversions_7d"]) <= 0:
                negatives.append(str(term["term"]).lower())

        irrelevant_spend = sum(item["spend_7d"] for item in audited if item["classification"] == "irrelevant")

        return {
            "tool": "search_terms_audit",
            "account_id": account_id,
            "summary": {
                "term_count": len(audited),
                "irrelevant_count": sum(1 for item in audited if item["classification"] == "irrelevant"),
                "irrelevant_spend": round(irrelevant_spend, 2),
            },
            "negative_keyword_candidates": sorted(set(negatives)),
            "terms": audited[:30],
        }

    def benchmark_account(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        benchmark: VerticalBenchmark = m["benchmark"]

        roas_delta_pct = _safe_div(m["roas_7d"] - benchmark.roas_healthy, max(benchmark.roas_healthy, 0.01)) * 100
        cpa_delta_pct = _safe_div(m["cpa_7d"] - benchmark.cpa_target, max(benchmark.cpa_target, 0.01)) * 100
        qs_delta = m["quality_avg"] - benchmark.quality_score_min

        account_score = 60.0
        account_score += _clamp(roas_delta_pct, -40.0, 40.0) * 0.6
        account_score -= _clamp(cpa_delta_pct, -40.0, 80.0) * 0.35
        account_score += _clamp(qs_delta * 8.0, -20.0, 20.0)
        account_score = round(_clamp(account_score, 0.0, 100.0), 2)

        return {
            "tool": "benchmark_account",
            "account_id": account_id,
            "vertical": m["vertical"],
            "benchmark_score": account_score,
            "account_metrics": {
                "roas_7d": round(m["roas_7d"], 3),
                "cpa_7d": round(m["cpa_7d"], 2),
                "quality_score_avg": round(m["quality_avg"], 2),
            },
            "vertical_targets": {
                "roas_healthy": benchmark.roas_healthy,
                "cpa_target": benchmark.cpa_target,
                "quality_score_min": benchmark.quality_score_min,
            },
            "delta": {
                "roas_delta_pct": round(roas_delta_pct, 2),
                "cpa_delta_pct": round(cpa_delta_pct, 2),
                "quality_score_delta": round(qs_delta, 2),
            },
        }

    def generate_negative_keywords(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        audit = self.search_terms_audit(account_id, {})
        candidates = audit["negative_keyword_candidates"]
        return {
            "tool": "generate_negative_keywords",
            "account_id": account_id,
            "count": len(candidates),
            "keywords": candidates,
            "source": "deterministic_search_term_filters",
        }

    def draft_campaign(self, account_id: int, params: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        vertical = m["vertical"]
        monthly_budget = float(params.get("monthly_budget", 3000))
        benchmark: VerticalBenchmark = m["benchmark"]
        target_geography = str(params.get("target_geography", "Local radius +25mi")).strip() or "Local radius +25mi"
        campaign_goal = str(params.get("campaign_goal", "Lead generation")).strip() or "Lead generation"
        account_name = str(m["account"]["name"])

        if vertical == "self_storage":
            groups = [
                ("Climate-Controlled Storage", ["climate controlled storage", "temperature controlled storage", "indoor storage units"]),
                ("Self Storage - General", ["self storage", "storage units near me", "cheap storage units", "monthly storage rental"]),
                ("Moving & Transition Storage", ["moving storage", "temporary storage", "short term storage"]),
                ("Business Storage", ["business storage", "commercial storage", "inventory storage"]),
            ]
            bid_strategy = "Max Conversions"
            ad_schedule = "Mon-Sun 6am-9pm"
            network = "Search only"
            vertical_defaults = "FL Storage"
        elif vertical == "day_spa":
            groups = [
                ("Facials", ["custom facials", "hydrafacial", "facial spa near me"]),
                ("Massage", ["deep tissue massage", "relaxation massage", "massage spa near me"]),
                ("Memberships", ["spa membership", "monthly spa membership", "self care membership"]),
                ("Gift Cards", ["spa gift card", "massage gift card", "facial gift certificate"]),
            ]
            bid_strategy = "Maximize Conversion Value"
            ad_schedule = "Tue-Sun 9am-8pm"
            network = "Search only"
            vertical_defaults = "Day Spa"
        elif vertical == "dental":
            groups = [
                ("Implants", ["dental implants", "tooth implants", "implant dentist"]),
                ("Emergency", ["emergency dentist", "urgent dental care", "same day dentist"]),
                ("Invisalign", ["invisalign", "clear aligners", "invisalign dentist"]),
                ("Cosmetic", ["cosmetic dentist", "veneers", "teeth whitening"]),
            ]
            bid_strategy = "Maximize Conversions"
            ad_schedule = "Mon-Sat 7am-8pm"
            network = "Search only"
            vertical_defaults = "Dental"
        else:
            groups = [
                ("Core Service", ["core service", "best provider", "service near me"]),
                ("High Intent", ["book now", "same day service", "trusted provider"]),
                ("Brand", [account_name.lower(), f"{account_name.lower()} reviews", f"{account_name.lower()} pricing"]),
                ("Competitive", ["top rated provider", "best local option", "service company"]),
            ]
            bid_strategy = "Maximize Conversions"
            ad_schedule = "Mon-Sat 8am-8pm"
            network = "Search only"
            vertical_defaults = "General"

        per_group_budget = round(monthly_budget / max(len(groups), 1), 2)
        generated_negatives = self.generate_negative_keywords(account_id, {})["keywords"][:8]
        existing_negatives = [item["keyword"] for item in self.db.list_negative_keywords(account_id)[:10]]
        shared_negatives = list(dict.fromkeys(existing_negatives + generated_negatives))
        kal_note = next(
            (item["memory_value"] for item in m["context_memory"] if item["memory_key"] == "seasonality_note"),
            f"{account_name} is under target on ROAS. New structure is recommended before expanding broad budget.",
        )
        stag_groups = [
            {
                "ad_group": group_name,
                "keywords": [
                    {
                        "text": keyword if vertical == "self_storage" else f"{keyword} {target_geography.split()[0].lower()}",
                        "match_type": ["exact", "phrase", "phrase"][index % 3],
                        "max_cpc": round(max(1.4, benchmark.cpa_target / max(benchmark.roas_healthy * 12, 1)) + (index * 0.3), 2),
                    }
                    for index, keyword in enumerate(seed_keywords)
                ],
                "monthly_budget": per_group_budget,
                "headlines": [
                    f"{group_name} | {account_name}",
                    f"Book {group_name} Today",
                    f"{target_geography} | {campaign_goal}",
                ],
                "descriptions": [
                    f"{account_name} offers {group_name.lower()} with clear pricing and fast response.",
                    f"Built with STAG methodology for stronger relevance and cleaner search-term control.",
                ],
            }
            for group_name, seed_keywords in groups
        ]

        return {
            "tool": "draft_campaign",
            "account_id": account_id,
            "status": "draft",
            "methodology": "STAG",
            "campaign_name": f"{account_name} - {campaign_goal} Draft",
            "vertical": vertical,
            "recommended_monthly_budget": round(monthly_budget, 2),
            "daily_budget": round(monthly_budget / 30.4, 2),
            "campaign_goal": campaign_goal,
            "campaign_type": "Search",
            "bid_strategy": bid_strategy,
            "target_cpa": round(benchmark.cpa_target, 2),
            "target_geography": target_geography,
            "network": network,
            "ad_schedule": ad_schedule,
            "vertical_defaults": vertical_defaults,
            "benchmark_comparison": {
                "portfolio_avg_cpa": benchmark.cpa_target,
                "vertical_avg_roas": benchmark.roas_healthy,
                "predicted_roas_min": round(max(benchmark.roas_healthy - 0.3, 1.2), 2),
                "predicted_roas_max": round(benchmark.roas_healthy + 0.2, 2),
            },
            "kal_note": kal_note,
            "shared_negatives": shared_negatives,
            "ad_groups": stag_groups,
            "responsive_search_ad": {
                "headlines": stag_groups[0]["headlines"],
                "descriptions": stag_groups[0]["descriptions"],
                "predicted_ad_strength": "Good (82%)",
            },
            "review_required": True,
        }

    def cross_mcc_anomalies(self, _: int, params: dict[str, Any]) -> dict[str, Any]:
        accounts = self.db.list_accounts()
        rows: list[dict[str, Any]] = []
        for account in accounts:
            metrics = self._account_metrics(int(account["id"]))
            rows.append(
                {
                    "account_id": account["id"],
                    "account_name": account["name"],
                    "vertical": account["vertical"],
                    "roas_7d": metrics["roas_7d"],
                    "roas_drop_pct": max(0.0, metrics["roas_drop_pct"]),
                    "waste_ratio": self.analyze_budget_waste(int(account["id"]), {})["components"]["waste_ratio"],
                }
            )

        if not rows:
            return {
                "tool": "cross_mcc_anomalies",
                "anomalies": [],
                "summary": "No active accounts",
            }

        roas_values = [row["roas_7d"] for row in rows]
        roas_mean = mean(roas_values)
        roas_std = math.sqrt(mean([(x - roas_mean) ** 2 for x in roas_values])) if len(rows) > 1 else 0.0

        anomalies: list[dict[str, Any]] = []
        for row in rows:
            z = _safe_div((row["roas_7d"] - roas_mean), roas_std if roas_std > 0 else 1)
            is_outlier = abs(z) >= 1.0 or row["roas_drop_pct"] >= 25 or row["waste_ratio"] >= 0.35
            if is_outlier:
                anomalies.append(
                    {
                        "account_id": row["account_id"],
                        "account_name": row["account_name"],
                        "vertical": row["vertical"],
                        "roas_7d": round(row["roas_7d"], 3),
                        "roas_drop_pct": round(row["roas_drop_pct"], 2),
                        "waste_ratio": round(row["waste_ratio"], 3),
                        "roas_zscore": round(z, 3),
                    }
                )

        return {
            "tool": "cross_mcc_anomalies",
            "summary": {
                "account_count": len(rows),
                "anomaly_count": len(anomalies),
                "mean_roas": round(roas_mean, 3),
            },
            "anomalies": anomalies,
        }

    def competitor_analysis(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        return {
            "tool": "competitor_analysis",
            "account_id": account_id,
            "status": "placeholder",
            "next_step": "Wire Google Ads Auction Insights endpoint once token permissions are active.",
        }

    def landing_page_audit(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        return {
            "tool": "landing_page_audit",
            "account_id": account_id,
            "status": "placeholder",
            "next_step": "Integrate crawler + analytics signals for bounce-rate and speed diagnostics.",
        }

    def keyword_expansion(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        return {
            "tool": "keyword_expansion",
            "account_id": account_id,
            "status": "placeholder",
            "next_step": "Attach query logs and deterministic expansion templates by vertical.",
        }

    def ad_copy_performance(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        return {
            "tool": "ad_copy_performance",
            "account_id": account_id,
            "status": "placeholder",
            "next_step": "Map ad group-level CTR/CVR split by message angle and seasonality.",
        }

    def pacing(self, account_id: int, _: dict[str, Any]) -> dict[str, Any]:
        m = self._account_metrics(account_id)
        spend_delta = m["spend_7d"] - m["spend_prev_7d"]
        return {
            "tool": "pacing",
            "account_id": account_id,
            "status": "placeholder",
            "weekly_spend_delta": round(spend_delta, 2),
            "next_step": "Add true month-to-date budget targets when account budget feed is wired.",
        }
