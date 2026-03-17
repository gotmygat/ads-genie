from __future__ import annotations

from typing import Any
import json

from .ads_client import GoogleAdsAdapter, GoogleAdsAPIError
from .db import ActionRecord, Database
from .observability import RuntimeMonitor


class ActionExecutor:
    def __init__(self, db: Database, ads: GoogleAdsAdapter, monitor: RuntimeMonitor) -> None:
        self.db = db
        self.ads = ads
        self.monitor = monitor

    def _live_add_negative_keywords(
        self,
        action: ActionRecord,
        account: dict[str, Any],
        keywords: list[str],
        source: str,
    ) -> dict[str, Any]:
        campaign_id = action.params.get("campaign_id")
        if campaign_id is None:
            raise ValueError("Live negative keyword action requires campaign_id")

        customer_id = str(account["customer_id"])
        preview = self.ads.add_campaign_negative_keywords(
            customer_id=customer_id,
            campaign_id=int(campaign_id),
            keywords=keywords,
            validate_only=True,
        )
        execute = self.ads.add_campaign_negative_keywords(
            customer_id=customer_id,
            campaign_id=int(campaign_id),
            keywords=keywords,
            validate_only=False,
        )
        added = len(keywords)
        message = f"Added {added}/{len(keywords)} campaign negative keywords in Google Ads"
        self.db.mark_action_status(action.id, "executed", message)
        self.db.insert_decision(
            account_id=action.account_id,
            actor=source,
            action="google_ads_write",
            payload={
                "action_id": action.id,
                "action_type": action.action_type,
                "preview": preview,
                "execute": execute,
            },
            alert_id=None,
        )
        return {
            "ok": True,
            "message": message,
            "added": added,
            "requested": len(keywords),
            "provider": "google_ads",
            "preview": preview,
            "execute": execute,
        }

    def _live_pause_campaign(self, action: ActionRecord, account: dict[str, Any], source: str) -> dict[str, Any]:
        campaign_id = int(action.params["campaign_id"])
        customer_id = str(account["customer_id"])
        preview = self.ads.pause_campaign(customer_id=customer_id, campaign_id=campaign_id, validate_only=True)
        execute = self.ads.pause_campaign(customer_id=customer_id, campaign_id=campaign_id, validate_only=False)
        message = f"Paused campaign {campaign_id} in Google Ads"
        self.db.mark_action_status(action.id, "executed", message)
        self.db.insert_decision(
            account_id=action.account_id,
            actor=source,
            action="google_ads_write",
            payload={
                "action_id": action.id,
                "action_type": action.action_type,
                "preview": preview,
                "execute": execute,
            },
            alert_id=None,
        )
        return {
            "ok": True,
            "message": message,
            "campaign_id": campaign_id,
            "provider": "google_ads",
            "preview": preview,
            "execute": execute,
        }

    def _live_adjust_bid(self, action: ActionRecord, account: dict[str, Any], source: str) -> dict[str, Any]:
        campaign_id = int(action.params["campaign_id"])
        pct_delta = float(action.params.get("pct_delta", 0.0))
        customer_id = str(account["customer_id"])
        preview = self.ads.adjust_campaign_bids(
            customer_id=customer_id,
            campaign_id=campaign_id,
            pct_delta=pct_delta,
            validate_only=True,
        )
        execute = self.ads.adjust_campaign_bids(
            customer_id=customer_id,
            campaign_id=campaign_id,
            pct_delta=pct_delta,
            validate_only=False,
        )
        message = f"Adjusted CPC bids by {pct_delta:+.2f}% across eligible ad groups in Google Ads"
        self.db.mark_action_status(action.id, "executed", message)
        self.db.insert_decision(
            account_id=action.account_id,
            actor=source,
            action="google_ads_write",
            payload={
                "action_id": action.id,
                "action_type": action.action_type,
                "preview": preview,
                "execute": execute,
            },
            alert_id=None,
        )
        return {
            "ok": True,
            "message": message,
            "campaign_id": campaign_id,
            "pct_delta": pct_delta,
            "provider": "google_ads",
            "preview": preview,
            "execute": execute,
        }

    def execute(self, action: ActionRecord, source: str = "system") -> dict[str, Any]:
        params = action.params
        action_type = action.action_type

        try:
            account = self.db.get_account(action.account_id) or {}
            is_live = str(account.get("data_source", "demo")).lower() == "live"
            self.monitor.emit(
                "info",
                "action_execute_start",
                f"Executing action {action_type}",
                account_id=action.account_id,
                action_id=action.id,
                details={"params": params, "is_live": is_live, "source": source},
            )

            if action_type == "add_negative_keywords":
                keywords = [str(k).strip().lower() for k in params.get("keywords", []) if str(k).strip()]
                if is_live:
                    result = self._live_add_negative_keywords(action, account, keywords, source)
                else:
                    added = 0
                    for keyword in keywords:
                        if self.db.add_negative_keyword(action.account_id, keyword, source=source):
                            added += 1
                    message = f"Added {added}/{len(keywords)} negative keywords"
                    self.db.mark_action_status(action.id, "executed", message)
                    result = {"ok": True, "message": message, "added": added, "requested": len(keywords), "provider": "local"}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "pause_campaign":
                campaign_id = int(params["campaign_id"])
                if is_live:
                    result = self._live_pause_campaign(action, account, source)
                else:
                    self.db.set_campaign_status(campaign_id, "paused")
                    message = f"Paused campaign {campaign_id}"
                    self.db.mark_action_status(action.id, "executed", message)
                    result = {"ok": True, "message": message, "campaign_id": campaign_id, "provider": "local"}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "adjust_bid":
                campaign_id = int(params["campaign_id"])
                pct_delta = float(params.get("pct_delta", 0.0))
                if is_live:
                    result = self._live_adjust_bid(action, account, source)
                else:
                    outcome = self.db.adjust_campaign_bid(campaign_id, pct_delta)
                    if not outcome:
                        raise ValueError(f"Campaign {campaign_id} not found")
                    message = f"Adjusted bid modifier by {pct_delta:+.2f}%"
                    self.db.mark_action_status(action.id, "executed", message)
                    result = {"ok": True, "message": message, "provider": "local", **outcome}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "draft_campaign":
                self.db.mark_action_status(action.id, "executed", "Draft prepared for human review")
                result = {
                    "ok": True,
                    "message": "Draft campaign retained in action payload for review",
                    "draft": params,
                    "provider": "local",
                }
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            raise ValueError(f"Unsupported action_type: {action_type}")
        except (GoogleAdsAPIError, ValueError, KeyError) as exc:
            self.db.mark_action_status(action.id, "failed", str(exc))
            self.monitor.exception(
                "action_execute_failed",
                exc,
                message=f"Action {action_type} failed",
                account_id=action.account_id,
                action_id=action.id,
                details={"params": params, "source": source},
            )
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            self.db.mark_action_status(action.id, "failed", str(exc))
            self.monitor.exception(
                "action_execute_failed",
                exc,
                message=f"Unexpected action failure for {action_type}",
                account_id=action.account_id,
                action_id=action.id,
                details={"params": params, "source": source},
            )
            return {"ok": False, "error": str(exc)}

