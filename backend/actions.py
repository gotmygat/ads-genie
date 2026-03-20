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

    def _mark_live_action_non_reversible(self, action: ActionRecord, message: str) -> None:
        params = dict(action.params)
        params["rollback"] = {
            "supported": False,
            "reason": "Live Google Ads rollback is not implemented for this action type.",
        }
        self.db.update_action_params(action.id, params)
        self.monitor.emit(
            "info",
            "action_rollback_metadata",
            message,
            account_id=action.account_id,
            action_id=action.id,
            details={"action_type": action.action_type},
        )

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
                    self._mark_live_action_non_reversible(action, "Stored non-reversible metadata for live negative keyword action")
                else:
                    added = 0
                    for keyword in keywords:
                        if self.db.add_negative_keyword(action.account_id, keyword, source=source):
                            added += 1
                    message = f"Added {added}/{len(keywords)} negative keywords"
                    self.db.mark_action_status(action.id, "executed", message)
                    params["rollback"] = {"supported": True, "keywords": keywords}
                    self.db.update_action_params(action.id, params)
                    result = {"ok": True, "message": message, "added": added, "requested": len(keywords), "provider": "local"}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "pause_campaign":
                campaign_id = int(params["campaign_id"])
                if is_live:
                    result = self._live_pause_campaign(action, account, source)
                    self._mark_live_action_non_reversible(action, "Stored non-reversible metadata for live pause action")
                else:
                    params["rollback"] = {"supported": True, "campaign_status_before": "enabled"}
                    self.db.set_campaign_status(campaign_id, "paused")
                    message = f"Paused campaign {campaign_id}"
                    self.db.mark_action_status(action.id, "executed", message)
                    self.db.update_action_params(action.id, params)
                    result = {"ok": True, "message": message, "campaign_id": campaign_id, "provider": "local"}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "adjust_bid":
                campaign_id = int(params["campaign_id"])
                pct_delta = float(params.get("pct_delta", 0.0))
                if is_live:
                    result = self._live_adjust_bid(action, account, source)
                    self._mark_live_action_non_reversible(action, "Stored non-reversible metadata for live bid adjustment")
                else:
                    outcome = self.db.adjust_campaign_bid(campaign_id, pct_delta)
                    if not outcome:
                        raise ValueError(f"Campaign {campaign_id} not found")
                    message = f"Adjusted bid modifier by {pct_delta:+.2f}%"
                    self.db.mark_action_status(action.id, "executed", message)
                    params["rollback"] = {
                        "supported": True,
                        "bid_modifier_before": outcome["bid_modifier_before"],
                    }
                    self.db.update_action_params(action.id, params)
                    result = {"ok": True, "message": message, "provider": "local", **outcome}
                self.monitor.emit("info", "action_execute_success", result["message"], account_id=action.account_id, action_id=action.id, details=result)
                return result

            if action_type == "draft_campaign":
                self.db.mark_action_status(action.id, "executed", "Draft prepared for human review")
                params["rollback"] = {"supported": True, "kind": "draft_discard"}
                self.db.update_action_params(action.id, params)
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

    def rollback(self, action: ActionRecord, source: str = "system") -> dict[str, Any]:
        params = dict(action.params)
        rollback = params.get("rollback", {})
        if not isinstance(rollback, dict):
            rollback = {}

        account = self.db.get_account(action.account_id) or {}
        is_live = str(account.get("data_source", "demo")).lower() == "live"
        if is_live or rollback.get("supported") is False:
            message = str(rollback.get("reason") or "Rollback is not supported for this action")
            self.db.mark_action_status(action.id, "rollback_blocked", message)
            return {"ok": False, "error": message}

        try:
            if action.action_type == "add_negative_keywords":
                keywords = [str(item).strip().lower() for item in rollback.get("keywords", params.get("keywords", [])) if str(item).strip()]
                removed = 0
                for keyword in keywords:
                    if self.db.remove_negative_keyword(action.account_id, keyword):
                        removed += 1
                message = f"Rolled back {removed}/{len(keywords)} negative keywords"
                self.db.mark_action_status(action.id, "rolled_back", message)
                return {"ok": True, "message": message, "removed": removed}

            if action.action_type == "pause_campaign":
                campaign_id = int(params["campaign_id"])
                campaign_status_before = str(rollback.get("campaign_status_before", "enabled"))
                self.db.set_campaign_status(campaign_id, campaign_status_before)
                message = f"Restored campaign {campaign_id} to {campaign_status_before}"
                self.db.mark_action_status(action.id, "rolled_back", message)
                return {"ok": True, "message": message, "campaign_id": campaign_id}

            if action.action_type == "adjust_bid":
                campaign_id = int(params["campaign_id"])
                previous = rollback.get("bid_modifier_before")
                if previous is None:
                    raise ValueError("Rollback metadata missing original bid modifier")
                outcome = self.db.set_campaign_bid_modifier(campaign_id, float(previous))
                if not outcome:
                    raise ValueError(f"Campaign {campaign_id} not found")
                message = f"Restored bid modifier to {float(previous):.4f}"
                self.db.mark_action_status(action.id, "rolled_back", message)
                return {"ok": True, "message": message, **outcome}

            if action.action_type == "draft_campaign":
                message = "Draft marked as discarded"
                self.db.mark_action_status(action.id, "rolled_back", message)
                return {"ok": True, "message": message}

            message = f"Rollback is not supported for action_type: {action.action_type}"
            self.db.mark_action_status(action.id, "rollback_blocked", message)
            return {"ok": False, "error": message}
        except (GoogleAdsAPIError, ValueError, KeyError) as exc:
            self.db.mark_action_status(action.id, "rollback_blocked", str(exc))
            self.monitor.exception(
                "action_rollback_failed",
                exc,
                message=f"Rollback failed for {action.action_type}",
                account_id=action.account_id,
                action_id=action.id,
                details={"params": params, "source": source},
            )
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            self.db.mark_action_status(action.id, "rollback_blocked", str(exc))
            self.monitor.exception(
                "action_rollback_failed",
                exc,
                message=f"Unexpected rollback failure for {action.action_type}",
                account_id=action.account_id,
                action_id=action.id,
                details={"params": params, "source": source},
            )
            return {"ok": False, "error": str(exc)}
