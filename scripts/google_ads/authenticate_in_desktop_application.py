#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_CLIENT_SECRETS = REPO_ROOT / "client_secrets.json"
TOKEN_HELPER = SCRIPT_DIR / "generate_user_credentials.py"


def main() -> int:
    if not TOKEN_HELPER.exists():
        print("Missing generate_user_credentials.py", file=sys.stderr)
        return 1

    if not DEFAULT_CLIENT_SECRETS.exists():
        print(
            "Missing client_secrets.json. Download your OAuth client secrets from Google Cloud and place it in the repository root or pass a custom path to the token helper.",
            file=sys.stderr,
        )
        return 1

    command = [
        sys.executable,
        str(TOKEN_HELPER),
        "--client_secrets_path",
        str(DEFAULT_CLIENT_SECRETS),
    ]
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
