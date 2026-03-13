from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import json
import sqlite3
import threading
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


@dataclass
class ActionRecord:
    id: int
    account_id: int
    action_type: str
    params: dict[str, Any]
    status: str


class Database:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.lock = threading.RLock()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self.lock:
            cur = self.conn.execute(sql, tuple(params))
            self.conn.commit()
            return cur

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        with self.lock:
            self.conn.executemany(sql, seq_of_params)
            self.conn.commit()

    def fetchone(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        with self.lock:
            cur = self.conn.execute(sql, tuple(params))
            row = cur.fetchone()
            return row_to_dict(row)

    def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.lock:
            cur = self.conn.execute(sql, tuple(params))
            return [row_to_dict(row) or {} for row in cur.fetchall()]

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.fetchall(f"PRAGMA table_info({table_name})")
        return {str(row["name"]) for row in rows}

    def _ensure_column(self, table_name: str, column_name: str, column_def: str) -> None:
        if column_name in self._table_columns(table_name):
            return
        self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    def init_schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    customer_id TEXT NOT NULL UNIQUE,
                    vertical TEXT NOT NULL,
                    timezone TEXT NOT NULL DEFAULT 'America/Toronto',
                    slack_channel TEXT,
                    data_source TEXT NOT NULL DEFAULT 'demo',
                    google_ads_customer_name TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    autonomy_json TEXT NOT NULL,
                    quiet_hours_start INTEGER,
                    quiet_hours_end INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'enabled',
                    spend_7d REAL NOT NULL,
                    spend_prev_7d REAL NOT NULL,
                    conversions_7d REAL NOT NULL,
                    conversions_prev_7d REAL NOT NULL,
                    revenue_7d REAL NOT NULL,
                    revenue_prev_7d REAL NOT NULL,
                    quality_score_avg REAL NOT NULL,
                    impression_share_lost_budget REAL NOT NULL,
                    cpc REAL NOT NULL DEFAULT 0,
                    bid_modifier REAL NOT NULL DEFAULT 1.0,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS search_terms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    campaign_id INTEGER,
                    term TEXT NOT NULL,
                    spend_7d REAL NOT NULL,
                    conversions_7d REAL NOT NULL,
                    clicks_7d INTEGER NOT NULL,
                    quality_score REAL NOT NULL,
                    relevance TEXT NOT NULL DEFAULT 'unknown',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
                    FOREIGN KEY(campaign_id) REFERENCES campaigns(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS negative_keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(account_id, keyword),
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    status TEXT NOT NULL,
                    autonomy_level TEXT NOT NULL,
                    requires_approval INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    recommendation_json TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id INTEGER,
                    account_id INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    executed_at TEXT,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
                    FOREIGN KEY(alert_id) REFERENCES alerts(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id INTEGER,
                    account_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
                    FOREIGN KEY(alert_id) REFERENCES alerts(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS client_context_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    memory_key TEXT NOT NULL,
                    memory_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(account_id, memory_key),
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER,
                    report_type TEXT NOT NULL,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    content_markdown TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS scheduler_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column("accounts", "data_source", "TEXT NOT NULL DEFAULT 'demo'")
            self._ensure_column("accounts", "google_ads_customer_name", "TEXT")
            self.conn.commit()

    def seed_demo_data(self) -> None:
        existing = self.fetchone("SELECT COUNT(*) AS count FROM accounts")
        if existing and int(existing["count"]) > 0:
            return

        now = utc_now_iso()
        autonomy = {
            "default": "propose_wait",
            "action_levels": {
                "add_negative_keywords": "auto_execute",
                "pause_campaign": "propose_wait",
                "adjust_bid": "propose_wait",
                "draft_campaign": "draft_review",
            },
            "escalation": {
                "spend_anomaly_pct": 50,
                "roas_drop_pct": 45,
            },
        }

        accounts = [
            (
                "PMS Self Storage",
                "100-100-1001",
                "self_storage",
                "America/Toronto",
                "#pms-ads",
                "demo",
                None,
                "active",
                json.dumps(autonomy),
                23,
                6,
                now,
                now,
            ),
            (
                "Riverfront Day Spa",
                "100-100-1002",
                "day_spa",
                "America/Toronto",
                "#spa-ads",
                "demo",
                None,
                "active",
                json.dumps(autonomy),
                22,
                7,
                now,
                now,
            ),
            (
                "Summit Dental",
                "100-100-1003",
                "dental",
                "America/Toronto",
                "#dental-ads",
                "demo",
                None,
                "active",
                json.dumps(autonomy),
                23,
                6,
                now,
                now,
            ),
        ]

        self.executemany(
            """
            INSERT INTO accounts (
                name, customer_id, vertical, timezone, slack_channel, data_source,
                google_ads_customer_name, status,
                autonomy_json, quiet_hours_start, quiet_hours_end, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            accounts,
        )

        seeded_accounts = self.fetchall("SELECT id, vertical FROM accounts ORDER BY id")
        account_ids = {row["vertical"]: row["id"] for row in seeded_accounts}

        campaigns = [
            (
                account_ids["self_storage"],
                "Storage Search Core",
                "enabled",
                1600,
                1450,
                18,
                17,
                5400,
                5100,
                7.8,
                0.18,
                3.9,
                1.0,
                now,
            ),
            (
                account_ids["self_storage"],
                "Storage Generic Expansion",
                "enabled",
                1120,
                840,
                2,
                8,
                980,
                2500,
                4.2,
                0.42,
                5.4,
                1.0,
                now,
            ),
            (
                account_ids["day_spa"],
                "Spa Brand",
                "enabled",
                700,
                680,
                24,
                23,
                4900,
                4700,
                8.5,
                0.09,
                2.1,
                1.0,
                now,
            ),
            (
                account_ids["day_spa"],
                "Spa Generic Treatments",
                "enabled",
                1310,
                900,
                7,
                13,
                1650,
                3150,
                5.1,
                0.31,
                4.8,
                1.0,
                now,
            ),
            (
                account_ids["dental"],
                "Dental Implants",
                "enabled",
                1900,
                1820,
                16,
                15,
                9800,
                9100,
                7.2,
                0.22,
                5.9,
                1.0,
                now,
            ),
            (
                account_ids["dental"],
                "Emergency Dental",
                "enabled",
                860,
                880,
                11,
                10,
                3900,
                3600,
                6.9,
                0.11,
                4.1,
                1.0,
                now,
            ),
        ]

        self.executemany(
            """
            INSERT INTO campaigns (
                account_id, name, status, spend_7d, spend_prev_7d, conversions_7d,
                conversions_prev_7d, revenue_7d, revenue_prev_7d, quality_score_avg,
                impression_share_lost_budget, cpc, bid_modifier, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            campaigns,
        )

        campaign_map = {
            row["name"]: row["id"]
            for row in self.fetchall("SELECT id, name FROM campaigns ORDER BY id")
        }

        search_terms = [
            (
                account_ids["self_storage"],
                campaign_map["Storage Generic Expansion"],
                "mini storage near me free",
                188,
                0,
                59,
                3.4,
                "irrelevant",
                now,
            ),
            (
                account_ids["self_storage"],
                campaign_map["Storage Generic Expansion"],
                "storage unit jobs",
                126,
                0,
                42,
                2.9,
                "irrelevant",
                now,
            ),
            (
                account_ids["self_storage"],
                campaign_map["Storage Search Core"],
                "storage unit auction",
                98,
                0,
                31,
                3.2,
                "irrelevant",
                now,
            ),
            (
                account_ids["day_spa"],
                campaign_map["Spa Generic Treatments"],
                "spa gift card discount",
                155,
                1,
                38,
                5.6,
                "borderline",
                now,
            ),
            (
                account_ids["day_spa"],
                campaign_map["Spa Generic Treatments"],
                "free spa jobs",
                142,
                0,
                47,
                3.8,
                "irrelevant",
                now,
            ),
            (
                account_ids["dental"],
                campaign_map["Dental Implants"],
                "cheap dental implants financing",
                310,
                2,
                74,
                6.1,
                "relevant",
                now,
            ),
            (
                account_ids["dental"],
                campaign_map["Dental Implants"],
                "dental school appointments",
                88,
                0,
                29,
                3.5,
                "irrelevant",
                now,
            ),
        ]

        self.executemany(
            """
            INSERT INTO search_terms (
                account_id, campaign_id, term, spend_7d, conversions_7d, clicks_7d,
                quality_score, relevance, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            search_terms,
        )

        memories = [
            (
                account_ids["self_storage"],
                "seasonality_note",
                "March traffic usually rises with moving season. Avoid over-reacting to CPC spikes under 8%.",
                now,
            ),
            (
                account_ids["day_spa"],
                "offer_constraint",
                "Client does not want deep discounts in ad copy; prioritize premium positioning.",
                now,
            ),
            (
                account_ids["dental"],
                "compliance",
                "Avoid guarantee language in medical ads. Review all new copy before publish.",
                now,
            ),
        ]
        self.executemany(
            """
            INSERT INTO client_context_memory (account_id, memory_key, memory_value, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            memories,
        )

    def list_accounts(self) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, name, customer_id, vertical, timezone, slack_channel, status,
                   data_source, google_ads_customer_name,
                   autonomy_json, quiet_hours_start, quiet_hours_end, created_at, updated_at
            FROM accounts
            ORDER BY id
            """
        )

    def get_account(self, account_id: int) -> dict[str, Any] | None:
        return self.fetchone(
            """
            SELECT id, name, customer_id, vertical, timezone, slack_channel, status,
                   data_source, google_ads_customer_name,
                   autonomy_json, quiet_hours_start, quiet_hours_end, created_at, updated_at
            FROM accounts
            WHERE id = ?
            """,
            (account_id,),
        )

    def create_account(
        self,
        name: str,
        customer_id: str,
        vertical: str,
        timezone_value: str,
        slack_channel: str,
        autonomy_json: str,
        data_source: str = "demo",
        google_ads_customer_name: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        cur = self.execute(
            """
            INSERT INTO accounts (
                name, customer_id, vertical, timezone, slack_channel, data_source,
                google_ads_customer_name, status,
                autonomy_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (
                name,
                customer_id,
                vertical,
                timezone_value,
                slack_channel,
                data_source,
                google_ads_customer_name,
                autonomy_json,
                now,
                now,
            ),
        )
        account_id = int(cur.lastrowid)
        return self.get_account(account_id) or {}

    def campaigns_for_account(self, account_id: int) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, account_id, name, status, spend_7d, spend_prev_7d, conversions_7d,
                   conversions_prev_7d, revenue_7d, revenue_prev_7d, quality_score_avg,
                   impression_share_lost_budget, cpc, bid_modifier, updated_at
            FROM campaigns
            WHERE account_id = ?
            ORDER BY id
            """,
            (account_id,),
        )

    def search_terms_for_account(self, account_id: int) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, account_id, campaign_id, term, spend_7d, conversions_7d,
                   clicks_7d, quality_score, relevance, created_at
            FROM search_terms
            WHERE account_id = ?
            ORDER BY spend_7d DESC, id ASC
            """,
            (account_id,),
        )

    def insert_alert(
        self,
        account_id: int,
        alert_type: str,
        severity: str,
        status: str,
        autonomy_level: str,
        requires_approval: bool,
        title: str,
        summary: str,
        recommendation: dict[str, Any],
        context: dict[str, Any],
    ) -> int:
        now = utc_now_iso()
        cur = self.execute(
            """
            INSERT INTO alerts (
                account_id, alert_type, severity, status, autonomy_level, requires_approval,
                title, summary, recommendation_json, context_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                alert_type,
                severity,
                status,
                autonomy_level,
                1 if requires_approval else 0,
                title,
                summary,
                json.dumps(recommendation),
                json.dumps(context),
                now,
                now,
            ),
        )
        return int(cur.lastrowid)

    def update_alert_status(self, alert_id: int, status: str) -> None:
        self.execute(
            "UPDATE alerts SET status = ?, updated_at = ? WHERE id = ?",
            (status, utc_now_iso(), alert_id),
        )

    def get_alert(self, alert_id: int) -> dict[str, Any] | None:
        return self.fetchone(
            """
            SELECT id, account_id, alert_type, severity, status, autonomy_level,
                   requires_approval, title, summary, recommendation_json, context_json,
                   created_at, updated_at
            FROM alerts
            WHERE id = ?
            """,
            (alert_id,),
        )

    def list_alerts(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if status:
            return self.fetchall(
                """
                SELECT id, account_id, alert_type, severity, status, autonomy_level,
                       requires_approval, title, summary, recommendation_json, context_json,
                       created_at, updated_at
                FROM alerts
                WHERE status = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (status, limit),
            )
        return self.fetchall(
            """
            SELECT id, account_id, alert_type, severity, status, autonomy_level,
                   requires_approval, title, summary, recommendation_json, context_json,
                   created_at, updated_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def insert_action(
        self,
        account_id: int,
        action_type: str,
        params: dict[str, Any],
        status: str,
        reason: str,
        alert_id: int | None = None,
    ) -> ActionRecord:
        now = utc_now_iso()
        cur = self.execute(
            """
            INSERT INTO actions (
                alert_id, account_id, action_type, params_json, status, reason,
                created_at, executed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_id,
                account_id,
                action_type,
                json.dumps(params),
                status,
                reason,
                now,
                now if status == "executed" else None,
            ),
        )
        return ActionRecord(
            id=int(cur.lastrowid),
            account_id=account_id,
            action_type=action_type,
            params=params,
            status=status,
        )

    def mark_action_status(self, action_id: int, status: str, reason: str) -> None:
        executed_at = utc_now_iso() if status in {"executed", "failed", "blocked"} else None
        self.execute(
            """
            UPDATE actions
            SET status = ?, reason = ?, executed_at = COALESCE(?, executed_at)
            WHERE id = ?
            """,
            (status, reason, executed_at, action_id),
        )

    def list_actions(self, account_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if account_id is None:
            return self.fetchall(
                """
                SELECT id, alert_id, account_id, action_type, params_json, status,
                       reason, created_at, executed_at
                FROM actions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        return self.fetchall(
            """
            SELECT id, alert_id, account_id, action_type, params_json, status,
                   reason, created_at, executed_at
            FROM actions
            WHERE account_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (account_id, limit),
        )

    def actions_for_alert(self, alert_id: int) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, alert_id, account_id, action_type, params_json, status,
                   reason, created_at, executed_at
            FROM actions
            WHERE alert_id = ?
            ORDER BY id ASC
            """,
            (alert_id,),
        )

    def insert_decision(
        self,
        account_id: int,
        actor: str,
        action: str,
        payload: dict[str, Any],
        alert_id: int | None = None,
    ) -> int:
        cur = self.execute(
            """
            INSERT INTO decisions (alert_id, account_id, actor, action, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (alert_id, account_id, actor, action, json.dumps(payload), utc_now_iso()),
        )
        return int(cur.lastrowid)

    def list_decisions(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, alert_id, account_id, actor, action, payload_json, created_at
            FROM decisions
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def add_negative_keyword(self, account_id: int, keyword: str, source: str) -> bool:
        try:
            self.execute(
                """
                INSERT INTO negative_keywords (account_id, keyword, source, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (account_id, keyword.strip().lower(), source, utc_now_iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def list_negative_keywords(self, account_id: int) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, account_id, keyword, source, created_at
            FROM negative_keywords
            WHERE account_id = ?
            ORDER BY created_at DESC
            """,
            (account_id,),
        )

    def set_campaign_status(self, campaign_id: int, status: str) -> None:
        self.execute(
            "UPDATE campaigns SET status = ?, updated_at = ? WHERE id = ?",
            (status, utc_now_iso(), campaign_id),
        )

    def adjust_campaign_bid(self, campaign_id: int, pct_delta: float) -> dict[str, Any] | None:
        campaign = self.fetchone(
            "SELECT id, bid_modifier FROM campaigns WHERE id = ?",
            (campaign_id,),
        )
        if not campaign:
            return None
        current = float(campaign["bid_modifier"])
        next_value = max(0.1, min(3.0, current * (1 + pct_delta / 100.0)))
        self.execute(
            "UPDATE campaigns SET bid_modifier = ?, updated_at = ? WHERE id = ?",
            (next_value, utc_now_iso(), campaign_id),
        )
        return {
            "campaign_id": campaign_id,
            "bid_modifier_before": round(current, 4),
            "bid_modifier_after": round(next_value, 4),
        }

    def write_report(
        self,
        report_type: str,
        period_start: str,
        period_end: str,
        content_markdown: str,
        account_id: int | None = None,
    ) -> int:
        cur = self.execute(
            """
            INSERT INTO reports (
                account_id, report_type, period_start, period_end, content_markdown, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (account_id, report_type, period_start, period_end, content_markdown, utc_now_iso()),
        )
        return int(cur.lastrowid)

    def latest_report(
        self,
        report_type: str,
        account_id: int | None = None,
    ) -> dict[str, Any] | None:
        if account_id is None:
            return self.fetchone(
                """
                SELECT id, account_id, report_type, period_start, period_end, content_markdown, generated_at
                FROM reports
                WHERE report_type = ?
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (report_type,),
            )
        return self.fetchone(
            """
            SELECT id, account_id, report_type, period_start, period_end, content_markdown, generated_at
            FROM reports
            WHERE report_type = ? AND account_id = ?
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (report_type, account_id),
        )

    def list_context_memory(self, account_id: int) -> list[dict[str, Any]]:
        return self.fetchall(
            """
            SELECT id, account_id, memory_key, memory_value, updated_at
            FROM client_context_memory
            WHERE account_id = ?
            ORDER BY memory_key ASC
            """,
            (account_id,),
        )

    def upsert_context_memory(self, account_id: int, key: str, value: str) -> None:
        self.execute(
            """
            INSERT INTO client_context_memory (account_id, memory_key, memory_value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(account_id, memory_key)
            DO UPDATE SET memory_value = excluded.memory_value, updated_at = excluded.updated_at
            """,
            (account_id, key, value, utc_now_iso()),
        )

    def get_scheduler_state(self, key: str) -> str | None:
        row = self.fetchone(
            "SELECT state_value FROM scheduler_state WHERE state_key = ?",
            (key,),
        )
        if not row:
            return None
        return str(row["state_value"])

    def set_scheduler_state(self, key: str, value: str) -> None:
        self.execute(
            """
            INSERT INTO scheduler_state (state_key, state_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key)
            DO UPDATE SET state_value = excluded.state_value, updated_at = excluded.updated_at
            """,
            (key, value, utc_now_iso()),
        )
