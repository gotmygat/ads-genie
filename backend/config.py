from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


DEFAULT_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def load_env_file(path: Path = DEFAULT_ENV_PATH) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    app_host: str
    app_port: int
    db_path: str
    timezone: str
    monitor_interval_seconds: int
    enable_scheduler: bool
    auto_seed: bool
    google_ads_developer_token: str
    google_ads_client_id: str
    google_ads_client_secret: str
    google_ads_refresh_token: str
    google_ads_login_customer_id: str
    google_ads_api_version: str
    slack_bot_token: str
    slack_signing_secret: str
    slack_default_channel: str
    claude_api_key: str

    @property
    def has_google_ads_credentials(self) -> bool:
        required = [
            self.google_ads_developer_token,
            self.google_ads_client_id,
            self.google_ads_client_secret,
            self.google_ads_refresh_token,
        ]
        return all(_is_real_secret(item) for item in required)

    @property
    def has_slack_credentials(self) -> bool:
        return bool(self.slack_bot_token and self.slack_signing_secret)



def _to_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _is_real_secret(value: str) -> bool:
    cleaned = (value or "").strip()
    if not cleaned:
        return False
    if cleaned.upper().startswith("REPLACE_ME"):
        return False
    return True


def load_settings() -> Settings:
    load_env_file()
    base_dir = Path(__file__).resolve().parent.parent
    return Settings(
        app_host=os.getenv("APP_HOST", "127.0.0.1"),
        app_port=int(os.getenv("APP_PORT", "8080")),
        db_path=os.getenv("DB_PATH", str(base_dir / "data" / "ads_genie.db")),
        timezone=os.getenv("APP_TIMEZONE", "America/Toronto"),
        monitor_interval_seconds=int(os.getenv("MONITOR_INTERVAL_SECONDS", "300")),
        enable_scheduler=_to_bool(os.getenv("ENABLE_SCHEDULER"), True),
        auto_seed=_to_bool(os.getenv("AUTO_SEED"), True),
        google_ads_developer_token=os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        google_ads_client_id=os.getenv("GOOGLE_ADS_CLIENT_ID", ""),
        google_ads_client_secret=os.getenv("GOOGLE_ADS_CLIENT_SECRET", ""),
        google_ads_refresh_token=os.getenv("GOOGLE_ADS_REFRESH_TOKEN", ""),
        google_ads_login_customer_id=os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""),
        google_ads_api_version=os.getenv("GOOGLE_ADS_API_VERSION", "v22"),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
        slack_signing_secret=os.getenv("SLACK_SIGNING_SECRET", ""),
        slack_default_channel=os.getenv("SLACK_DEFAULT_CHANNEL", ""),
        claude_api_key=os.getenv("CLAUDE_API_KEY", ""),
    )
