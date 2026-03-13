from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any
import json
import logging
import os
import time

try:
    import boto3
except ImportError:  # pragma: no cover - dependency installed in target runtime
    boto3 = None  # type: ignore[assignment]

try:
    import httpx
except ImportError:  # pragma: no cover - dependency installed in target runtime
    httpx = None  # type: ignore[assignment]

try:
    import structlog
except ImportError:  # pragma: no cover - fallback for bare environments
    structlog = None

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:  # pragma: no cover - dependency installed in target runtime
    GoogleAdsClient = None  # type: ignore[assignment]

    class GoogleAdsException(Exception):
        """Fallback exception used when google-ads is unavailable."""


try:
    from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential
except ImportError:  # pragma: no cover - fallback for bare environments
    def retry(*_args: Any, **_kwargs: Any):  # type: ignore[misc]
        def decorator(fn: Any) -> Any:
            return fn

        return decorator

    def retry_if_exception(_fn: Any) -> Any:
        return None

    def stop_after_attempt(_attempts: int) -> None:
        return None

    def wait_exponential(**_kwargs: Any) -> None:
        return None

    def before_sleep_log(_logger: Any, _level: Any = None, **_kwargs: Any):  # type: ignore[misc]
        def _callback(_retry_state: Any) -> None:
            return None

        return _callback


LOGGER = structlog.get_logger(__name__) if structlog else logging.getLogger(__name__)
RETRY_LOGGER = logging.getLogger(__name__)
TRANSIENT_ERROR_CODES = {"INTERNAL_ERROR", "TEMPORARY_UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED"}


class RateLimitExceeded(RuntimeError):
    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__(f"Rate limit exceeded; retry after {retry_after_seconds} seconds")
        self.retry_after_seconds = retry_after_seconds


class SecretConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class GoogleAdsCredentials:
    client_id: str
    client_secret: str
    developer_token: str
    refresh_token: str
    mcc_customer_id: str


class RateLimiter:
    def __init__(self, max_requests_per_minute: int = 30) -> None:
        self.max_requests_per_minute = max_requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = RLock()

    def acquire(self, customer_id: str) -> None:
        now = time.time()
        with self._lock:
            bucket = self._requests[customer_id]
            while bucket and now - bucket[0] >= 60:
                bucket.popleft()
            if len(bucket) >= self.max_requests_per_minute:
                retry_after_seconds = max(1, int(60 - (now - bucket[0])))
                raise RateLimitExceeded(retry_after_seconds=retry_after_seconds)
            bucket.append(now)


def _is_transient_google_ads_error(exc: BaseException) -> bool:
    if not isinstance(exc, GoogleAdsException):
        return False
    failure = getattr(exc, "failure", None)
    errors = getattr(failure, "errors", []) if failure is not None else []
    for error in errors:
        error_code = getattr(error, "error_code", None)
        if error_code is None:
            continue
        code_name = getattr(error_code, "WhichOneof", lambda *_args: None)("error_code")
        if code_name and code_name.upper() in TRANSIENT_ERROR_CODES:
            return True
    return False


class GoogleAdsAuth:
    """
    Authentication and client factory for Google Ads.

    Production mode loads credentials from AWS Secrets Manager. Local mode may read directly
    from the environment to make development possible without cloud dependencies.
    """

    def __init__(self) -> None:
        self.environment = os.getenv("ENV", "local").strip().lower()
        self.region_name = os.getenv("AWS_REGION", "us-east-1")
        self.secret_name = os.getenv("GOOGLE_ADS_SECRET_NAME", "ads-genie/google-ads")
        self._credentials = self._load_credentials()
        self._lock = RLock()
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self.rate_limiter = RateLimiter(max_requests_per_minute=int(os.getenv("GOOGLE_ADS_MAX_RPM", "30")))

    def _load_credentials(self) -> GoogleAdsCredentials:
        if self.environment == "production":
            return self._load_credentials_from_secrets_manager()
        return self._load_credentials_from_env()

    def _load_credentials_from_env(self) -> GoogleAdsCredentials:
        return GoogleAdsCredentials(
            client_id=os.getenv("GOOGLE_ADS_CLIENT_ID", "").strip(),
            client_secret=os.getenv("GOOGLE_ADS_CLIENT_SECRET", "").strip(),
            developer_token=os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip(),
            refresh_token=os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "").strip(),
            mcc_customer_id=os.getenv("GOOGLE_ADS_MCC_CUSTOMER_ID", "").strip(),
        )

    def _load_credentials_from_secrets_manager(self) -> GoogleAdsCredentials:
        if boto3 is None:
            raise SecretConfigurationError("boto3 dependency is required for Secrets Manager access")
        secrets = boto3.client("secretsmanager", region_name=self.region_name)
        response = secrets.get_secret_value(SecretId=self.secret_name)
        payload = json.loads(response["SecretString"])
        return GoogleAdsCredentials(
            client_id=str(payload["GOOGLE_ADS_CLIENT_ID"]).strip(),
            client_secret=str(payload["GOOGLE_ADS_CLIENT_SECRET"]).strip(),
            developer_token=str(payload["GOOGLE_ADS_DEVELOPER_TOKEN"]).strip(),
            refresh_token=str(payload["GOOGLE_ADS_REFRESH_TOKEN"]).strip(),
            mcc_customer_id=str(payload["GOOGLE_ADS_MCC_CUSTOMER_ID"]).strip(),
        )

    def _client_config(self) -> dict[str, Any]:
        config = {
            "developer_token": self._credentials.developer_token,
            "client_id": self._credentials.client_id,
            "client_secret": self._credentials.client_secret,
            "refresh_token": self._credentials.refresh_token,
            "use_proto_plus": True,
        }
        if self._credentials.mcc_customer_id:
            config["login_customer_id"] = self._credentials.mcc_customer_id
        return config

    def _ensure_google_ads_dependency(self) -> None:
        if GoogleAdsClient is None:
            raise SecretConfigurationError("google-ads dependency is not installed in this environment")

    def get_client(self, customer_id: str) -> Any:
        self.rate_limiter.acquire(customer_id)
        self.refresh_token_if_needed()
        self._ensure_google_ads_dependency()
        return GoogleAdsClient.load_from_dict(self._client_config())

    def get_mcc_client(self) -> Any:
        if not self._credentials.mcc_customer_id:
            raise SecretConfigurationError("GOOGLE_ADS_MCC_CUSTOMER_ID is required for MCC operations")
        return self.get_client(self._credentials.mcc_customer_id)

    def refresh_token_if_needed(self) -> None:
        with self._lock:
            now = datetime.now(timezone.utc)
            if self._token_expiry and now + timedelta(minutes=5) < self._token_expiry:
                return
            if httpx is None:
                raise SecretConfigurationError("httpx dependency is required for OAuth token refresh")

            response = httpx.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self._credentials.client_id,
                    "client_secret": self._credentials.client_secret,
                    "refresh_token": self._credentials.refresh_token,
                    "grant_type": "refresh_token",
                },
                timeout=20.0,
            )
            response.raise_for_status()
            payload = response.json()
            self._access_token = str(payload["access_token"])
            expires_in = int(payload.get("expires_in", 3600))
            self._token_expiry = now + timedelta(seconds=expires_in)

    @retry(
        retry=retry_if_exception(_is_transient_google_ads_error),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
        before_sleep=before_sleep_log(RETRY_LOGGER, logging.WARNING),
        reraise=True,
    )
    def list_accessible_accounts(self) -> list[dict[str, Any]]:
        client = self.get_mcc_client()
        service = client.get_service("CustomerService")
        resources = service.list_accessible_customers().resource_names
        accounts: list[dict[str, Any]] = []
        for resource_name in resources:
            customer_id = resource_name.split("/")[-1]
            query = (
                "SELECT customer.id, customer.descriptive_name, customer.currency_code, "
                "customer.time_zone FROM customer LIMIT 1"
            )
            google_ads_service = client.get_service("GoogleAdsService")
            response = google_ads_service.search(customer_id=customer_id, query=query)
            for row in response:
                customer = row.customer
                accounts.append(
                    {
                        "customer_id": str(customer.id),
                        "descriptive_name": customer.descriptive_name,
                        "currency_code": customer.currency_code,
                        "time_zone": customer.time_zone,
                    }
                )
        return accounts
