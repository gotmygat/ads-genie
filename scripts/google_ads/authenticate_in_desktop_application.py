#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import os
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_CLIENT_SECRETS = REPO_ROOT / "client_secrets.json"
TOKEN_HELPER = SCRIPT_DIR / "generate_user_credentials.py"


def _client_secrets_path() -> Path:
    env_value = os.getenv("GOOGLE_CLIENT_SECRETS_PATH", "").strip()
    if env_value:
        return Path(env_value).expanduser()
    return DEFAULT_CLIENT_SECRETS


def main() -> int:
    if not TOKEN_HELPER.exists():
        print("Missing generate_user_credentials.py", file=sys.stderr)
        return 1

    client_secrets = _client_secrets_path()
    if not client_secrets.exists():
        print(
            "Missing client_secrets.json. Download your OAuth client secrets from Google Cloud and place it in the repository root or set GOOGLE_CLIENT_SECRETS_PATH.",
            file=sys.stderr,
        )
        return 1

    command = [
        sys.executable,
        str(TOKEN_HELPER),
        "--client_secrets_path",
        str(client_secrets),
    ]
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
