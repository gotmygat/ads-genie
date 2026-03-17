# Ads Genie

Ads Genie is a Google Ads monitoring and actioning system with two parallel implementation paths in this repository:

- `backend/` + `frontend/`: the local runnable app you can use today
- `mcp_server/` + `orchestration/` + `slack_bot/` + `infrastructure/`: the production-oriented scaffold aligned to the planning docs

This repo is not a polished single-runtime product yet. It is a working local control-center app plus a partially implemented AWS/MCP production path.

## Current Status

### Working now
- Local Python app served from `backend/server.py`
- Command-center UI in `frontend/`
- SQLite-backed accounts, alerts, actions, decisions, reports, and runtime events
- Core deterministic analysis tools
- Optional live Google Ads read connectivity
- Partial live Google Ads write plumbing for low-risk actions
- Local auth, observability, and Slack interactivity support

### Not finished yet
- Full production deployment path
- Golden-account validation fixtures
- Real Slack workspace validation
- Real-client validation of live Google Ads writes
- Completion of all planned placeholder tools

## Quick Start

### 1. Install dependencies

```bash
git clone https://github.com/gotmygat/ads-genie.git
cd ads-genie
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
```

### 2. Run the local app

```bash
python3 -m backend.server
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080)

### 3. Run tests

```bash
.venv/bin/pytest tests -q
```

Current known passing state:
- `25 passed`

## Connecting Google Ads

Fill in `.env` with:

```env
GOOGLE_ADS_DEVELOPER_TOKEN=
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=
GOOGLE_ADS_REFRESH_TOKEN=
GOOGLE_ADS_MCC_CUSTOMER_ID=
```

Then restart the app and use:
- `GET /api/google-ads/test`
- `GET /api/google-ads/customers`
- `POST /api/google-ads/import-account`

## Repository Layout

```text
backend/         Local runnable backend, API, SQLite integration, orchestration
frontend/        Browser UI served by the local backend
mcp_server/      FastAPI MCP-style tool server and write actions
orchestration/   Lambda handlers, models, and Step Functions definitions
slack_bot/       Slack control-plane modules
infrastructure/  AWS CDK stacks and constructs
memory/          Decision/context memory modules
reports/         Weekly/monthly reporting modules
tests/           Unit and integration tests
data/            Local SQLite database
docs/            Planning docs, extracted references, project notes
scripts/         Helper scripts such as Google Ads OAuth tooling
```

## Docs

Start here for project context:
- [Project handoff](docs/project/AI_HANDOFF.md)
- [Task memory](docs/project/TASKS_LEFT.md)
- [Changelog](CHANGELOG.md)
- [Docs index](docs/README.md)

Planning/reference material:
- [Architecture review PDF](docs/planning/Ads-Genie-Architecture-Review-and-Strategic-Recommendations.pdf)
- [Implementation plan PDF](docs/planning/Ads-Genie-Implementation-Plan.pdf)
- [Architecture extract](docs/reference/architecture.txt)
- [Implementation extract](docs/reference/implementation.txt)

## Runtime Notes

- The local app defaults to demo mode when Google Ads credentials are not configured.
- The local SQLite database is tracked for now because the app was built around a seeded local demo flow.
- The production-oriented AWS/MCP path exists in code but is not the same thing as the local runnable app.
- Vercel is not the natural deployment target for the current local runtime because it depends on a long-lived server process, local SQLite, and an in-process scheduler.

## Next Recommended Work

1. Prepare a one-client live pilot.
2. Isolate demo noise from real imported account data.
3. Validate read-path outputs against the real Google Ads UI.
4. Test one low-risk live write action with validate-first behavior.
5. Decide whether the production target remains AWS-first.
