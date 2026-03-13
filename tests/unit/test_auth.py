from __future__ import annotations

import importlib
from unittest.mock import Mock, patch


def test_google_ads_auth_uses_secrets_manager_in_production(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("GOOGLE_ADS_CLIENT_ID", "env-client-id-should-not-be-used")
    monkeypatch.setenv("GOOGLE_ADS_CLIENT_SECRET", "env-client-secret-should-not-be-used")
    monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "env-developer-token-should-not-be-used")
    monkeypatch.setenv("GOOGLE_ADS_REFRESH_TOKEN", "env-refresh-token-should-not-be-used")
    monkeypatch.setenv("GOOGLE_ADS_MCC_CUSTOMER_ID", "env-mcc-id-should-not-be-used")
    monkeypatch.setenv("GOOGLE_ADS_SECRET_NAME", "ads-genie/google-ads")

    fake_client = Mock()
    fake_client.get_secret_value.return_value = {
        "SecretString": (
            '{"GOOGLE_ADS_CLIENT_ID":"secret-client-id","GOOGLE_ADS_CLIENT_SECRET":"secret-client-secret",'
            '"GOOGLE_ADS_DEVELOPER_TOKEN":"secret-developer-token","GOOGLE_ADS_REFRESH_TOKEN":"secret-refresh-token",'
            '"GOOGLE_ADS_MCC_CUSTOMER_ID":"1234567890"}'
        )
    }
    with patch("boto3.client", return_value=fake_client) as boto_client:
        module = importlib.import_module("mcp_server.auth.google_oauth")
        importlib.reload(module)
        auth = module.GoogleAdsAuth()

        boto_client.assert_called_once()
        assert auth._credentials.client_id == "secret-client-id"
        assert auth._credentials.client_secret == "secret-client-secret"
        assert auth._credentials.developer_token == "secret-developer-token"
        assert auth._credentials.refresh_token == "secret-refresh-token"
        assert auth._credentials.mcc_customer_id == "1234567890"
