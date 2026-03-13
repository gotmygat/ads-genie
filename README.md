# Ads Genie

Ads Genie is a production-oriented Google Ads monitoring and actioning system with three layers:

1. Deterministic analysis tools (`mcp_server/`)
2. Event-driven orchestration (`orchestration/`)
3. Human control plane (`frontend/`, `slack_bot/`)

This repository contains two runnable paths:

- `backend/` + `frontend/`: the local end-to-end app you can run today for demo mode and staged live Google Ads reads
- `mcp_server/`, `orchestration/`, `slack_bot/`, `infrastructure/`: the production architecture scaffold and implementation modules aligned to the architecture review and implementation plan

## Current Status

What works now:

- Local Python web app on port `8080`
- Demo account registry, alerts, actions, decisions, context memory, and reports using SQLite
- Google Ads live read connectivity via OAuth refresh token
- Command-center UI with:
  - account rail
  - KPI dashboard
  - alert feed
  - approve / modify / dismiss workflow
  - campaign draft review inspector
- Deterministic tool implementations for:
  - `health_check`
  - `analyze_budget_waste`
  - `diagnose_roas_drop`
  - `search_terms_audit`
  - `benchmark_account`
  - `generate_negative_keywords`
  - `draft_campaign`
  - `cross_mcc_anomalies`
- Write-action modules with autonomy enforcement in `mcp_server/write_actions/`
- Slack approval/task-token scaffolding
- Step Functions workflow definitions
- Unit and integration test coverage for the implemented core paths

What is still not production-complete:

- Golden-account validation fixtures and threshold calibration
- Live Slack interactive deployment and callback verification
- Full AWS deployment and runtime verification
- Live Google Ads write mutations verified against a real account
- Production auth around the local dashboard/API

## Repository Layout

```text
backend/          Local runnable app, API server, DB, orchestration, reports
frontend/         Command-center web UI served by backend/server.py
mcp_server/       Deterministic MCP-style tools and write actions
orchestration/    Lambda handlers, models, and Step Functions definitions
slack_bot/        Slack control plane, approvals, token bridge, messages
memory/           Decision memory and client context modules
reports/          Weekly/monthly reporting modules
infrastructure/   AWS CDK stacks and constructs
tests/            Unit and integration tests
data/             Local SQLite database
```

## Local Development

### Requirements

- Python `3.11+`
- Git
- Google Ads credentials if you want live read mode
- Slack credentials only if you want to test Slack posting

### Setup

```bash
cd "/Users/kalhawari/Documents/Ads Genie"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
```

### Run the local app

```bash
python3 -m backend.server
```

Open:

- [http://127.0.0.1:8080](http://127.0.0.1:8080)

Notes:

- The app defaults to demo mode if Google Ads credentials are not configured.
- The scheduler runs automatically in local mode unless disabled in config.

### Run tests

```bash
.venv/bin/pytest tests -q
```

Current known passing state:

- `25 passed`

## Environment Variables

The repo includes `.env.example`. The main values you will actually use first are:

```env
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=
GOOGLE_ADS_DEVELOPER_TOKEN=
GOOGLE_ADS_REFRESH_TOKEN=
GOOGLE_ADS_MCC_CUSTOMER_ID=
SLACK_BOT_TOKEN=
SLACK_APP_TOKEN=
SLACK_SIGNING_SECRET=
ANTHROPIC_API_KEY=
AWS_REGION=
```

For the local app, the Google Ads read path primarily uses the values in `.env`.

## Connecting Google Ads

### What you need

- Google Ads developer token
- Google Cloud OAuth client ID and secret
- Refresh token with `https://www.googleapis.com/auth/adwords`
- Optional MCC customer ID if you are operating through a manager account

### Steps

1. Fill in `.env`:

```env
GOOGLE_ADS_DEVELOPER_TOKEN=...
GOOGLE_ADS_CLIENT_ID=...
GOOGLE_ADS_CLIENT_SECRET=...
GOOGLE_ADS_REFRESH_TOKEN=...
GOOGLE_ADS_MCC_CUSTOMER_ID=...
```

2. Restart the server:

```bash
python3 -m backend.server
```

3. In the UI:
- use the Google Ads connection controls if you are on the classic dashboard flow
- or work through the API endpoints directly

### Relevant local API endpoints

- `GET /api/google-ads/test`
- `GET /api/google-ads/customers`
- `POST /api/google-ads/import-account`

## Local API Endpoints

Main local app endpoints:

- `GET /api/health`
- `GET /api/accounts`
- `GET /api/accounts/{id}`
- `GET /api/accounts/{id}/campaigns`
- `GET /api/accounts/{id}/negatives`
- `GET /api/alerts`
- `POST /api/alerts/{id}/decision`
- `GET /api/actions`
- `GET /api/decisions`
- `GET /api/tools`
- `POST /api/tools/run`
- `POST /api/run-monitoring`
- `GET /api/reports/weekly/latest`
- `POST /api/reports/weekly/generate`
- `GET /api/reports/monthly/{account_id}/latest`
- `POST /api/reports/monthly/generate`
- `GET /api/context/{account_id}`
- `POST /api/context/{account_id}`

## MCP Server

The custom MCP-style server is implemented in `mcp_server/` and exposed via FastAPI.

Main file:

- `mcp_server/server.py`

The server includes:

- inbound validation
- outbound policy validation
- dependency health checks
- structured tool invocation logging
- Lambda compatibility through `Mangum`

## Slack Control Plane

Slack modules live in `slack_bot/`.

Implemented pieces:

- alert Block Kit builders
- approval / modify / dismiss handlers
- task token storage
- app mention query routing
- Slack signature verification helper

Important:

- Slack is scaffolded in code, but still needs live app configuration and deployment verification.

## AWS Deployment

### Current deployment posture

The CDK stacks in `infrastructure/` are scaffolded and partially implemented. They are suitable as a starting point, not as a fully validated production deployment artifact.

### Intended production components

- Lambda for MCP server or ECS if runtime grows
- Step Functions for approval workflows
- EventBridge schedules
- DynamoDB tables for:
  - accounts
  - autonomy config
  - decisions
  - cache
  - task tokens
  - MCC benchmarks
  - MCC negatives
- S3 audit bucket with Object Lock
- Secrets Manager for Google Ads and Slack credentials

### To prepare for deployment

1. Install CDK dependencies if not already present.
2. Configure AWS credentials locally.
3. Create secrets in AWS Secrets Manager.
4. Fill environment/table names as needed.
5. Synthesize and review stacks before deploy.

Typical flow:

```bash
cd "/Users/kalhawari/Documents/Ads Genie"
python3 -m pip install aws-cdk-lib constructs
cdk synth
cdk deploy
```

### Required production checks before real use

- Secrets are sourced from Secrets Manager, not plaintext env in production
- Slack signing verification is active end-to-end
- Step Functions task-token callbacks work from Slack buttons
- Decision log dual-write to DynamoDB + S3 is verified
- CloudWatch alarms and tracing are enabled and tested
- Golden-account accuracy checks are in place

## Git / Publishing

Remote repository:

- [https://github.com/gotmygat/ads-genie](https://github.com/gotmygat/ads-genie)

Main branch:

- `main`

## Security Notes

- `.env` is ignored by Git
- No secrets should be committed
- Production should use Secrets Manager exclusively
- The local dashboard is not production-authenticated yet

## Next Recommended Work

1. Add the 10 golden account fixtures and calibrate outputs.
2. Replace prompt-based modify actions in the UI with real in-app forms/modals.
3. Verify live Google Ads write mutations on a controlled test account.
4. Deploy Slack + Step Functions end-to-end in AWS.
5. Lock down production auth and channel/account isolation.
