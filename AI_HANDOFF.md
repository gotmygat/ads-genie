# Ads Genie Handoff For Another AI

Use this file as the primary handoff document. It is meant to replace the need for full chat history.

## Start Here

Project root:
- `/Users/kalhawari/Documents/Ads Genie`

Primary handoff file:
- `/Users/kalhawari/Documents/Ads Genie/AI_HANDOFF.md`

Supporting files:
- `/Users/kalhawari/Documents/Ads Genie/TASKS_LEFT.md`
- `/Users/kalhawari/Documents/Ads Genie/README.md`

GitHub repo:
- `https://github.com/gotmygat/ads-genie`

Current branch:
- `main`

Latest important app/UI commit:
- `7c5d3cf` - `feat: top nav tabs, inline modify, optimistic UI, loading states`

## What The User Wants

The user wants Ads Genie built end-to-end so it is genuinely functional:
- backend
- frontend
- real Google Ads data
- eventually safe real actions

The user accepts placeholders for secrets and credentials, but does not want fake architecture-only output. The user prefers direct execution over planning-heavy responses.

## What Was Built

There are two parallel code paths in this repo:

### 1. Local runnable app

This is what the user is actually opening in the browser.

Files:
- `/Users/kalhawari/Documents/Ads Genie/backend/server.py`
- `/Users/kalhawari/Documents/Ads Genie/backend/db.py`
- `/Users/kalhawari/Documents/Ads Genie/backend/ads_client.py`
- `/Users/kalhawari/Documents/Ads Genie/backend/tools.py`
- `/Users/kalhawari/Documents/Ads Genie/backend/orchestrator.py`
- `/Users/kalhawari/Documents/Ads Genie/frontend/index.html`
- `/Users/kalhawari/Documents/Ads Genie/frontend/styles.css`
- `/Users/kalhawari/Documents/Ads Genie/frontend/app.js`

Characteristics:
- Runs locally on port `8080`
- Uses SQLite at `/Users/kalhawari/Documents/Ads Genie/data/ads_genie.db`
- Mixes seeded demo data with optional live Google Ads account imports
- This is the current working demo/control-center UI

### 2. Production-oriented architecture scaffold

Files/directories:
- `/Users/kalhawari/Documents/Ads Genie/mcp_server`
- `/Users/kalhawari/Documents/Ads Genie/orchestration`
- `/Users/kalhawari/Documents/Ads Genie/slack_bot`
- `/Users/kalhawari/Documents/Ads Genie/infrastructure`
- `/Users/kalhawari/Documents/Ads Genie/memory`
- `/Users/kalhawari/Documents/Ads Genie/reports`

Characteristics:
- More aligned to the architecture docs
- Includes MCP-style tools, orchestration handlers, Slack scaffolding, and CDK scaffolds
- Not fully deployed or fully validated end-to-end

## What Is Real Vs Fake Right Now

### Real-capable now

These can work with real Google Ads data once credentials are configured:
- Google Ads connection test
- listing accessible customers
- importing one real account into the app
- read-oriented analysis against that imported account

Relevant endpoints:
- `GET /api/google-ads/test`
- `GET /api/google-ads/customers`
- `POST /api/google-ads/import-account`

### Still fake, demo, or partially simulated

- seeded alerts in SQLite
- seeded decisions/history in SQLite
- a lot of dashboard content before live import
- some orchestration/recommendation flows
- Slack end-to-end production behavior
- AWS production deployment path
- fully trusted live write execution path

## Current Frontend State

The user asked to replace the browser UI files with provided versions. That happened.

Files replaced directly:
- `/Users/kalhawari/Documents/Ads Genie/frontend/index.html`
- `/Users/kalhawari/Documents/Ads Genie/frontend/styles.css`
- `/Users/kalhawari/Documents/Ads Genie/frontend/app.js`

Commit:
- `7c5d3cf` - `feat: top nav tabs, inline modify, optimistic UI, loading states`

UI intent:
- top nav tabs
- inline modify flow
- optimistic UI
- loading states

## Important Context About Data

The current UI should not be treated as fully real data.

Why:
- the local DB is seeded with demo accounts and demo alerts
- live Google Ads import is optional and not yet the default state
- some recommendations/alerts are generated locally from demo-like flows

If another AI is asked "is the data real?", the correct answer is:
- partially real-capable, but currently still heavily demo unless a live account is imported and demo noise is isolated

## Minimum Work Needed To Test One Real Client

This is the most important practical next step.

1. Put real Google Ads credentials into:
   - `/Users/kalhawari/Documents/Ads Genie/.env`

2. Required values:
   - `GOOGLE_ADS_DEVELOPER_TOKEN`
   - `GOOGLE_ADS_CLIENT_ID`
   - `GOOGLE_ADS_CLIENT_SECRET`
   - `GOOGLE_ADS_REFRESH_TOKEN`
   - `GOOGLE_ADS_MCC_CUSTOMER_ID`

3. Restart:
   - `python3 -m backend.server`

4. Verify:
   - `GET /api/google-ads/test`
   - `GET /api/google-ads/customers`

5. Import exactly one live account:
   - `POST /api/google-ads/import-account`

6. Prefer a clean test database or isolate demo data so the user can clearly distinguish real vs fake

7. Validate read-path outputs against the actual Google Ads UI:
   - health check
   - budget waste
   - ROAS diagnosis
   - search terms audit

8. Only after read verification, test one low-risk write action with approval and validation-first behavior

## Recommended Next Engineering Task

Prepare a clean one-client live pilot.

Suggested order:
1. back up existing SQLite DB
2. either reset or isolate demo data
3. connect real Google Ads credentials
4. import one live client
5. verify dashboard/account/campaign metrics against Google Ads UI
6. test one safe write path

## Why Vercel Failed

The user tried Vercel deployment.

Observed issue:
- Vercel could not find a FastAPI entrypoint

Correct explanation:
- the FastAPI app exists at `/Users/kalhawari/Documents/Ads Genie/mcp_server/server.py`
- Vercel was not finding it in its expected entrypoint locations
- the local app the user actually runs is not even FastAPI; it is a long-running custom server in `/Users/kalhawari/Documents/Ads Genie/backend/server.py`

Additional reasons this repo is not directly Vercel-ready as a full app:
- long-running `ThreadingHTTPServer`
- local SQLite persistence
- in-process scheduler thread

## Running The Current Local App

Command:
```bash
cd "/Users/kalhawari/Documents/Ads Genie"
python3 -m backend.server
```

URL:
- `http://127.0.0.1:8080`

Important note:
- starting it inside the agent sandbox was unreliable for browser reachability
- running it outside the sandbox or directly from the user's terminal was more reliable

## Files Another AI Should Read First

In this order:

1. `/Users/kalhawari/Documents/Ads Genie/AI_HANDOFF.md`
2. `/Users/kalhawari/Documents/Ads Genie/TASKS_LEFT.md`
3. `/Users/kalhawari/Documents/Ads Genie/README.md`
4. `/Users/kalhawari/Documents/Ads Genie/backend/server.py`
5. `/Users/kalhawari/Documents/Ads Genie/backend/db.py`
6. `/Users/kalhawari/Documents/Ads Genie/backend/ads_client.py`
7. `/Users/kalhawari/Documents/Ads Genie/backend/tools.py`
8. `/Users/kalhawari/Documents/Ads Genie/frontend/app.js`

## Important Project History

### Earlier build work completed

- local backend API and UI
- deterministic tool layer
- SQLite schema and seeded data
- orchestration layer
- reports
- Slack bridge scaffolding
- MCP-style server
- infrastructure scaffolds

### GitHub history cleanup completed

- history was cleaned and simplified earlier
- README was upgraded to a real operator/deployment guide

### Recent user-driven frontend replacement

- the user later provided replacement frontend files
- those were copied exactly over the previous frontend files
- no reinterpretation or redesign was intended in that step

## User Instructions Worth Preserving

- do not over-explain
- act directly when possible
- user wants the app genuinely functional, not just architected
- user may continue in another AI due to token limits
- user asked that remaining tasks be remembered

## Remaining High-Level Tasks

These are the practical tasks still left:

1. connect real Google Ads data fully for one client
2. isolate/remove demo data for a clean pilot
3. verify read-path accuracy against real account metrics
4. test one safe write action with human approval
5. wire real Slack interaction if desired
6. choose proper deployment target
7. only after that, harden toward production

## Current Local File State To Be Aware Of

As of this handoff:
- `AI_HANDOFF.md` is a new local file
- `data/ads_genie.db` may be locally modified from app runs

Another AI should check `git status` before making changes.

## Short Prompt To Give Another AI

Use this if the user wants to continue elsewhere:

> Open `/Users/kalhawari/Documents/Ads Genie/AI_HANDOFF.md` first. This repo has a runnable local app in `backend/` + `frontend/` and a separate production-oriented scaffold in `mcp_server/`, `orchestration/`, `slack_bot/`, and `infrastructure/`. The immediate goal is not architecture work; it is to get one real Google Ads client connected with minimal demo noise and validate the real read path in the current browser app.

