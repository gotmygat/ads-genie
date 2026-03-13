from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import threading
import time

from .config import Settings
from .db import Database


@dataclass
class AccountSnapshot:
    account: dict[str, Any]
    campaigns: list[dict[str, Any]]
    search_terms: list[dict[str, Any]]
    context_memory: list[dict[str, Any]]


class GoogleAdsAPIError(RuntimeError):
    pass


def _normalize_customer_id(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        raise ValueError(f"Invalid Google Ads customer id: {value}")
    return digits


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _get_nested(source: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in source:
            return source[key]
    return default


class GoogleAdsAdapter:
    """
    Google Ads API adapter with local fallback.

    - If credentials are configured and account `data_source` is `live`, reads come from
      Google Ads REST API.
    - Otherwise, data comes from local SQLite demo cache.
    """

    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self._token_lock = threading.RLock()
        self._token: str | None = None
        self._token_expiry_epoch: float = 0.0
        self._snapshot_cache: dict[int, tuple[float, AccountSnapshot]] = {}

    @property
    def mode(self) -> str:
        return "live" if self.settings.has_google_ads_credentials else "demo"

    @property
    def _api_base(self) -> str:
        return f"https://googleads.googleapis.com/{self.settings.google_ads_api_version}"

    @property
    def _login_customer_id(self) -> str | None:
        raw = (self.settings.google_ads_login_customer_id or "").strip()
        if not raw:
            return None
        return _normalize_customer_id(raw)

    def _oauth_token(self) -> str:
        with self._token_lock:
            now = time.time()
            if self._token and now < self._token_expiry_epoch - 60:
                return self._token

            payload = urlencode(
                {
                    "client_id": self.settings.google_ads_client_id,
                    "client_secret": self.settings.google_ads_client_secret,
                    "refresh_token": self.settings.google_ads_refresh_token,
                    "grant_type": "refresh_token",
                }
            ).encode("utf-8")
            request = Request(
                "https://oauth2.googleapis.com/token",
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urlopen(request, timeout=15) as response:
                    body = json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise GoogleAdsAPIError(f"OAuth token refresh failed ({exc.code}): {detail}") from exc
            except URLError as exc:
                raise GoogleAdsAPIError(f"OAuth token refresh failed: {exc}") from exc

            token = str(body.get("access_token", "")).strip()
            if not token:
                raise GoogleAdsAPIError(f"OAuth token missing in response: {body}")

            expires_in = _to_int(body.get("expires_in"), default=3600)
            self._token = token
            self._token_expiry_epoch = now + max(300, expires_in)
            return token

    def _ads_headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._oauth_token()}",
            "developer-token": self.settings.google_ads_developer_token,
            "Content-Type": "application/json; charset=utf-8",
        }
        login_customer_id = self._login_customer_id
        if login_customer_id:
            headers["login-customer-id"] = login_customer_id
        return headers

    def _request_json(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any] | None = None,
    ) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(url=url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise GoogleAdsAPIError(f"Google Ads API call failed ({exc.code}): {detail}") from exc
        except URLError as exc:
            raise GoogleAdsAPIError(f"Google Ads API call failed: {exc}") from exc

    def _search_stream(self, customer_id: str, query: str) -> list[dict[str, Any]]:
        url = f"{self._api_base}/customers/{customer_id}/googleAds:searchStream"
        response = self._request_json(
            method="POST",
            url=url,
            headers=self._ads_headers(),
            payload={"query": query},
        )

        batches: list[dict[str, Any]]
        if isinstance(response, list):
            batches = response
        elif isinstance(response, dict):
            batches = [response]
        else:
            raise GoogleAdsAPIError(f"Unexpected searchStream payload: {response}")

        rows: list[dict[str, Any]] = []
        for batch in batches:
            batch_rows = batch.get("results", [])
            if isinstance(batch_rows, list):
                rows.extend([row for row in batch_rows if isinstance(row, dict)])
        return rows

    def list_accessible_customer_ids(self) -> list[str]:
        if not self.settings.has_google_ads_credentials:
            return []

        url = f"{self._api_base}/customers:listAccessibleCustomers"
        response = self._request_json(
            method="GET",
            url=url,
            headers=self._ads_headers(),
            payload=None,
        )
        resource_names = response.get("resourceNames", []) if isinstance(response, dict) else []
        ids: list[str] = []
        for resource_name in resource_names:
            if not isinstance(resource_name, str):
                continue
            if resource_name.startswith("customers/"):
                ids.append(_normalize_customer_id(resource_name.split("/", 1)[1]))
        return sorted(set(ids))

    def describe_customer(self, customer_id: str) -> dict[str, Any]:
        cid = _normalize_customer_id(customer_id)
        query = (
            "SELECT customer.id, customer.descriptive_name, customer.currency_code, "
            "customer.time_zone FROM customer LIMIT 1"
        )
        rows = self._search_stream(cid, query)
        if not rows:
            return {
                "customer_id": cid,
                "descriptive_name": "",
                "currency_code": "",
                "time_zone": "",
            }

        customer = rows[0].get("customer", {})
        return {
            "customer_id": str(_get_nested(customer, "id", default=cid)),
            "descriptive_name": str(_get_nested(customer, "descriptiveName", "descriptive_name", default="")),
            "currency_code": str(_get_nested(customer, "currencyCode", "currency_code", default="")),
            "time_zone": str(_get_nested(customer, "timeZone", "time_zone", default="")),
        }

    def list_accessible_customers(self) -> list[dict[str, Any]]:
        customers: list[dict[str, Any]] = []
        for customer_id in self.list_accessible_customer_ids():
            try:
                info = self.describe_customer(customer_id)
                info["error"] = None
                customers.append(info)
            except Exception as exc:
                customers.append(
                    {
                        "customer_id": customer_id,
                        "descriptive_name": "",
                        "currency_code": "",
                        "time_zone": "",
                        "error": str(exc),
                    }
                )
        return customers

    def test_connection(self, customer_id: str | None = None) -> dict[str, Any]:
        if not self.settings.has_google_ads_credentials:
            return {"configured": False, "ok": False, "reason": "missing_credentials"}

        try:
            accessible = self.list_accessible_customer_ids()
            if customer_id:
                chosen = _normalize_customer_id(customer_id)
            elif accessible:
                chosen = accessible[0]
            else:
                chosen = self._login_customer_id

            if not chosen:
                return {
                    "configured": True,
                    "ok": False,
                    "reason": "no_accessible_customers",
                    "accessible_customer_ids": accessible,
                }

            summary = self.describe_customer(chosen)
            rows = self._search_stream(
                chosen,
                "SELECT campaign.id FROM campaign WHERE campaign.status != 'REMOVED' LIMIT 1",
            )
            return {
                "configured": True,
                "ok": True,
                "api_version": self.settings.google_ads_api_version,
                "login_customer_id": self._login_customer_id,
                "selected_customer_id": chosen,
                "accessible_customer_ids": accessible,
                "customer": summary,
                "sample_campaign_rows": len(rows),
            }
        except Exception as exc:
            return {
                "configured": True,
                "ok": False,
                "api_version": self.settings.google_ads_api_version,
                "login_customer_id": self._login_customer_id,
                "error": str(exc),
            }

    def _is_live_account(self, account: dict[str, Any]) -> bool:
        if self.mode != "live":
            return False
        return str(account.get("data_source", "demo")).lower() == "live"

    def _period_key(self, date_text: str) -> str | None:
        try:
            d = datetime.fromisoformat(date_text).date()
        except ValueError:
            return None

        today = date.today()
        current_start = today - timedelta(days=7)
        current_end = today - timedelta(days=1)
        prev_start = today - timedelta(days=14)
        prev_end = today - timedelta(days=8)

        if current_start <= d <= current_end:
            return "current"
        if prev_start <= d <= prev_end:
            return "previous"
        return None

    def _load_live_campaigns(self, account_id: int, customer_id: str) -> list[dict[str, Any]]:
        query = (
            "SELECT campaign.id, campaign.name, campaign.status, segments.date, "
            "metrics.cost_micros, metrics.conversions, metrics.conversions_value, "
            "metrics.average_cpc, metrics.search_budget_lost_impression_share "
            "FROM campaign "
            "WHERE campaign.status != 'REMOVED' AND segments.date DURING LAST_14_DAYS"
        )
        rows = self._search_stream(customer_id, query)

        quality_by_campaign: dict[str, tuple[float, float]] = {}
        quality_query = (
            "SELECT campaign.id, ad_group_criterion.quality_info.quality_score, metrics.impressions, segments.date "
            "FROM keyword_view "
            "WHERE ad_group_criterion.status != 'REMOVED' AND segments.date DURING LAST_14_DAYS"
        )
        try:
            quality_rows = self._search_stream(customer_id, quality_query)
            for row in quality_rows:
                campaign = row.get("campaign", {})
                campaign_id = str(_get_nested(campaign, "id", default="")).strip()
                if not campaign_id:
                    continue
                ad_group_criterion = row.get("adGroupCriterion", {})
                quality_info = ad_group_criterion.get("qualityInfo", {}) if isinstance(ad_group_criterion, dict) else {}
                score = _to_float(_get_nested(quality_info, "qualityScore", "quality_score", default=0.0), default=0.0)
                if score <= 0:
                    continue
                impressions = _to_float(row.get("metrics", {}).get("impressions", 1), default=1.0)
                weighted_sum, weight_total = quality_by_campaign.get(campaign_id, (0.0, 0.0))
                quality_by_campaign[campaign_id] = (weighted_sum + score * impressions, weight_total + impressions)
        except Exception:
            # Quality score is optional in fallback mode.
            quality_by_campaign = {}

        grouped: dict[str, dict[str, Any]] = {}
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        for row in rows:
            campaign = row.get("campaign", {})
            metrics = row.get("metrics", {})
            segments = row.get("segments", {})

            campaign_id = str(_get_nested(campaign, "id", default="")).strip()
            if not campaign_id:
                continue
            period = self._period_key(str(_get_nested(segments, "date", default="")))
            if not period:
                continue

            if campaign_id not in grouped:
                raw_status = str(_get_nested(campaign, "status", default="ENABLED")).upper()
                mapped_status = "enabled" if raw_status == "ENABLED" else "paused"
                grouped[campaign_id] = {
                    "id": _to_int(campaign_id),
                    "account_id": account_id,
                    "name": str(_get_nested(campaign, "name", default=f"Campaign {campaign_id}")),
                    "status": mapped_status,
                    "spend_7d": 0.0,
                    "spend_prev_7d": 0.0,
                    "conversions_7d": 0.0,
                    "conversions_prev_7d": 0.0,
                    "revenue_7d": 0.0,
                    "revenue_prev_7d": 0.0,
                    "quality_score_avg": 6.0,
                    "impression_share_lost_budget": 0.0,
                    "cpc": 0.0,
                    "bid_modifier": 1.0,
                    "updated_at": now_iso,
                }

            spend = _to_float(_get_nested(metrics, "costMicros", "cost_micros", default=0.0)) / 1_000_000.0
            conversions = _to_float(_get_nested(metrics, "conversions", default=0.0))
            revenue = _to_float(_get_nested(metrics, "conversionsValue", "conversions_value", default=0.0))
            avg_cpc_micros = _to_float(_get_nested(metrics, "averageCpc", "average_cpc", default=0.0))
            avg_cpc = avg_cpc_micros / 1_000_000.0 if avg_cpc_micros > 1000 else avg_cpc_micros
            lost_budget = _to_float(
                _get_nested(metrics, "searchBudgetLostImpressionShare", "search_budget_lost_impression_share", default=0.0)
            )

            target = grouped[campaign_id]
            if period == "current":
                target["spend_7d"] += spend
                target["conversions_7d"] += conversions
                target["revenue_7d"] += revenue
            else:
                target["spend_prev_7d"] += spend
                target["conversions_prev_7d"] += conversions
                target["revenue_prev_7d"] += revenue

            target["cpc"] = max(target["cpc"], avg_cpc)
            target["impression_share_lost_budget"] = max(target["impression_share_lost_budget"], lost_budget)

        for campaign_id, payload in grouped.items():
            weighted = quality_by_campaign.get(campaign_id)
            if weighted and weighted[1] > 0:
                payload["quality_score_avg"] = round(weighted[0] / weighted[1], 2)

            payload["spend_7d"] = round(payload["spend_7d"], 2)
            payload["spend_prev_7d"] = round(payload["spend_prev_7d"], 2)
            payload["conversions_7d"] = round(payload["conversions_7d"], 4)
            payload["conversions_prev_7d"] = round(payload["conversions_prev_7d"], 4)
            payload["revenue_7d"] = round(payload["revenue_7d"], 2)
            payload["revenue_prev_7d"] = round(payload["revenue_prev_7d"], 2)
            payload["cpc"] = round(payload["cpc"], 4)
            payload["impression_share_lost_budget"] = max(0.0, min(1.0, payload["impression_share_lost_budget"]))

        return sorted(grouped.values(), key=lambda item: item["spend_7d"], reverse=True)

    def _load_live_search_terms(
        self,
        account_id: int,
        customer_id: str,
        campaigns: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT campaign.id, search_term_view.search_term, segments.date, "
            "metrics.cost_micros, metrics.conversions, metrics.clicks "
            "FROM search_term_view "
            "WHERE segments.date DURING LAST_7_DAYS"
        )
        try:
            rows = self._search_stream(customer_id, query)
        except Exception:
            return []

        campaign_quality = {str(item["id"]): _to_float(item.get("quality_score_avg"), default=6.0) for item in campaigns}
        suspicious_tokens = {"free", "jobs", "career", "auction", "cheap", "template", "diy"}
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        for row in rows:
            campaign = row.get("campaign", {})
            search_term_view = row.get("searchTermView", {})
            metrics = row.get("metrics", {})
            segments = row.get("segments", {})

            if self._period_key(str(_get_nested(segments, "date", default=""))) != "current":
                continue

            campaign_id = str(_get_nested(campaign, "id", default="")).strip()
            term = str(_get_nested(search_term_view, "searchTerm", "search_term", default="")).strip()
            if not campaign_id or not term:
                continue

            key = (campaign_id, term.lower())
            if key not in grouped:
                grouped[key] = {
                    "id": len(grouped) + 1,
                    "account_id": account_id,
                    "campaign_id": _to_int(campaign_id),
                    "term": term,
                    "spend_7d": 0.0,
                    "conversions_7d": 0.0,
                    "clicks_7d": 0,
                    "quality_score": campaign_quality.get(campaign_id, 6.0),
                    "relevance": "relevant",
                    "created_at": now_iso,
                }

            target = grouped[key]
            target["spend_7d"] += _to_float(_get_nested(metrics, "costMicros", "cost_micros", default=0.0)) / 1_000_000.0
            target["conversions_7d"] += _to_float(_get_nested(metrics, "conversions", default=0.0))
            target["clicks_7d"] += _to_int(_get_nested(metrics, "clicks", default=0))

        for item in grouped.values():
            term_lower = str(item["term"]).lower()
            if any(token in term_lower for token in suspicious_tokens) and item["conversions_7d"] <= 0:
                item["relevance"] = "irrelevant"
            elif item["conversions_7d"] <= 0 and item["spend_7d"] >= 75:
                item["relevance"] = "borderline"
            item["spend_7d"] = round(item["spend_7d"], 2)
            item["conversions_7d"] = round(item["conversions_7d"], 4)
            item["quality_score"] = round(_to_float(item["quality_score"], 6.0), 2)

        return sorted(grouped.values(), key=lambda x: x["spend_7d"], reverse=True)

    def _fetch_live_snapshot(self, account: dict[str, Any]) -> AccountSnapshot:
        account_id = int(account["id"])
        customer_id = _normalize_customer_id(str(account["customer_id"]))

        campaigns = self._load_live_campaigns(account_id, customer_id)
        search_terms = self._load_live_search_terms(account_id, customer_id, campaigns)
        context_memory = self.db.list_context_memory(account_id)

        account_copy = dict(account)
        account_copy["customer_id"] = customer_id
        account_copy["snapshot_source"] = "google_ads_live"
        return AccountSnapshot(
            account=account_copy,
            campaigns=campaigns,
            search_terms=search_terms,
            context_memory=context_memory,
        )

    def _fallback_snapshot(self, account: dict[str, Any], live_error: str | None = None) -> AccountSnapshot:
        account_id = int(account["id"])
        campaigns = self.db.campaigns_for_account(account_id)
        search_terms = self.db.search_terms_for_account(account_id)
        context_memory = self.db.list_context_memory(account_id)
        account_copy = dict(account)
        account_copy["snapshot_source"] = "local_cache"
        if live_error:
            account_copy["live_error"] = live_error
        return AccountSnapshot(
            account=account_copy,
            campaigns=campaigns,
            search_terms=search_terms,
            context_memory=context_memory,
        )

    def fetch_account_snapshot(self, account_id: int) -> AccountSnapshot:
        account = self.db.get_account(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")

        if not self._is_live_account(account):
            return self._fallback_snapshot(account)

        now = time.time()
        cached = self._snapshot_cache.get(account_id)
        if cached and now - cached[0] <= 45:
            return cached[1]

        try:
            snapshot = self._fetch_live_snapshot(account)
            self._snapshot_cache[account_id] = (now, snapshot)
            return snapshot
        except Exception as exc:
            # Never break monitoring for an account because live API fails.
            return self._fallback_snapshot(account, live_error=str(exc))
