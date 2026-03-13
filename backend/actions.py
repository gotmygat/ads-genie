from __future__ import annotations

from typing import Any

from .db import ActionRecord, Database


class ActionExecutor:
    def __init__(self, db: Database) -> None:
        self.db = db

    def execute(self, action: ActionRecord, source: str = "system") -> dict[str, Any]:
        params = action.params
        action_type = action.action_type

        try:
            account = self.db.get_account(action.account_id) or {}
            is_live = str(account.get("data_source", "demo")).lower() == "live"
            if is_live and action_type in {"add_negative_keywords", "pause_campaign", "adjust_bid"}:
                message = "Live write actions are not enabled yet; action blocked for safety"
                self.db.mark_action_status(action.id, "blocked", message)
                return {"ok": False, "error": message, "blocked": True}

            if action_type == "add_negative_keywords":
                keywords = [str(k).strip().lower() for k in params.get("keywords", []) if str(k).strip()]
                added = 0
                for keyword in keywords:
                    if self.db.add_negative_keyword(action.account_id, keyword, source=source):
                        added += 1
                message = f"Added {added}/{len(keywords)} negative keywords"
                self.db.mark_action_status(action.id, "executed", message)
                return {"ok": True, "message": message, "added": added, "requested": len(keywords)}

            if action_type == "pause_campaign":
                campaign_id = int(params["campaign_id"])
                self.db.set_campaign_status(campaign_id, "paused")
                message = f"Paused campaign {campaign_id}"
                self.db.mark_action_status(action.id, "executed", message)
                return {"ok": True, "message": message, "campaign_id": campaign_id}

            if action_type == "adjust_bid":
                campaign_id = int(params["campaign_id"])
                pct_delta = float(params.get("pct_delta", 0.0))
                outcome = self.db.adjust_campaign_bid(campaign_id, pct_delta)
                if not outcome:
                    raise ValueError(f"Campaign {campaign_id} not found")
                message = f"Adjusted bid modifier by {pct_delta:+.2f}%"
                self.db.mark_action_status(action.id, "executed", message)
                return {"ok": True, "message": message, **outcome}

            if action_type == "draft_campaign":
                self.db.mark_action_status(action.id, "executed", "Draft prepared for human review")
                return {
                    "ok": True,
                    "message": "Draft campaign retained in action payload for review",
                    "draft": params,
                }

            raise ValueError(f"Unsupported action_type: {action_type}")
        except Exception as exc:
            self.db.mark_action_status(action.id, "failed", str(exc))
            return {"ok": False, "error": str(exc)}
