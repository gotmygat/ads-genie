# Ads Genie AI Handoff

Use this document first when another AI continues work in this repository.
It is the canonical summary of what the user asked for, what was implemented, what remains incomplete, and what the next AI should do next.

## Start Here

Project root:
- `ads-genie`

Primary files to read first:
1. `docs/project/AI_HANDOFF.md`
2. `docs/project/TASKS_LEFT.md`
3. `CHANGELOG.md`
4. `README.md`
5. `backend/server.py`
6. `backend/db.py`
7. `backend/ads_client.py`
8. `backend/tools.py`
9. `backend/orchestrator.py`
10. `frontend/app.js`

GitHub repo:
- `https://github.com/gotmygat/ads-genie`

Branch:
- `main`

Latest relevant commit before the current cleanup push:
- `7a1809e` - `feat: add auth, observability, slack approvals, and live write plumbing`

## Mandatory Documentation Maintenance Rule

At the end of every completed task, update these files together:
- `CHANGELOG.md`
- `docs/project/AI_HANDOFF.md`
- `docs/project/TASKS_LEFT.md`

Each end-of-task update must include:
- what changed
- what was validated
- what remains blocked
- the exact next recommended task
- who should pick it up next: `same AI` or `any AI`

Do not leave those documents stale after making implementation changes.

## Current Repo Reality

This repository has two parallel implementation paths.

### 1. Local runnable system

This is the path the user actually opens in a browser today.

Main files:
- `backend/server.py`
- `backend/db.py`
- `backend/ads_client.py`
- `backend/tools.py`
- `backend/orchestrator.py`
- `frontend/index.html`
- `frontend/styles.css`
- `frontend/app.js`

Characteristics:
- local HTTP server
- SQLite-backed state
- seeded demo accounts/alerts/history by default
- optional live Google Ads connectivity when credentials are configured
- currently the only dependable runtime path for user testing

### 2. Production-oriented scaffold

Directories:
- `mcp_server/`
- `orchestration/`
- `slack_bot/`
- `infrastructure/`
- `memory/`
- `reports/`

Characteristics:
- more aligned with the architecture and implementation-plan docs
- includes FastAPI MCP server, AWS Step Functions definitions, CDK stacks, Slack bot scaffolding, and deterministic tool modules
- not fully deployed or validated end-to-end

## User Goals Since Inception

The user repeatedly asked for Ads Genie to be built as a genuinely functional system, not a fake architecture exercise.

Core user requirements across the thread:
- build the project end-to-end from backend to frontend
- leave API keys as placeholders if needed, but make the system function locally
- make the app runnable in the browser
- keep memory of what tasks remain
- connect real Google Ads data
- support safe real Google Ads actions
- use Slack as a real approval/control plane eventually
- push work to GitHub
- keep a useful handoff for another AI if the user runs low on tokens
- document what is left and whether the original plan was truly implemented
- keep the repository and README looking organized on GitHub

The user prefers direct execution over speculative planning.

## Chronological Work Log

### Phase 0 - Source documents supplied

The user supplied high-level planning documents for architecture and implementation, including:
- Ads Genie architecture review PDF
- Ads Genie implementation plan PDF
- Google API design documentation

Those documents drove the intended shape of the repo.

### Phase 1 - Initial full-stack build

User ask:
- build the entire project from beginning to end, backend to frontend, fully functioning where possible without secrets

What was done:
1. created the repository structure
2. implemented the local backend in `backend/`
3. implemented the local frontend in `frontend/`
4. created the SQLite schema and seeded demo data
5. added the deterministic tool engine
6. added local orchestration and reporting
7. added a Slack bridge
8. added an MCP-style server path under `mcp_server/`
9. added AWS-oriented scaffolding under `orchestration/`, `slack_bot/`, and `infrastructure/`
10. added tests for the implemented core paths

Main commit:
- `523cf33` - `Build Ads Genie application stack`

### Phase 2 - Local run support and browser access

User ask:
- run the server so the app can be opened in a browser

What happened:
1. local app was started multiple times
2. there were repeated browser reachability issues because background processes launched from the agent environment were not always stable from the user’s browser session
3. the local app path remained the working runtime for UI viewing once started correctly

Important lesson for another AI:
- the local app is reliable when run directly from the user’s environment, but agent-launched background servers may appear healthy to the agent while still being inaccessible to the user browser

### Phase 3 - Task memory and Google Ads onboarding work

User asks:
- remember what tasks are left
- explain how to connect Google Ads
- clarify what campaign types the current tool supports

What was done:
1. created and updated task memory
2. wired Google Ads read-path support in the local app
3. documented the env vars and API endpoints needed to test live access
4. clarified that advanced optimization support is strongest for Search while read-level visibility can span broader campaign types

### Phase 4 - Master build prompt alignment and production-architecture scaffolding

User ask:
- align the repo to a detailed master build prompt describing MCP tools, AWS orchestration, Slack control plane, memory, reports, testing, and CDK deployment

What was done:
1. filled out `mcp_server/` with custom tool modules and write-action modules
2. added DynamoDB/Secrets/GAQL-oriented auth/query/cache modules
3. added orchestration Lambdas and Step Functions definitions
4. added Slack bot handlers, message builders, and task-token bridge scaffolding
5. added memory and reporting modules
6. added CDK stacks
7. added unit and integration tests around those paths

Important reality check:
- this work created substantial production-oriented code, but it did not fully replace the local runnable app as the active system

### Phase 5 - UI direction changes and GitHub publishing

User asks:
- implement a more opinionated UI/control center
- publish the repository to GitHub
- clean up the git history and improve the README

What was done:
1. published the repo to `https://github.com/gotmygat/ads-genie`
2. cleaned the branch history into a simpler linear story
3. upgraded the README into a real setup/deployment/operator guide
4. later replaced the local frontend files with user-provided versions that introduced:
   - top nav tabs
   - inline modify
   - optimistic UI behavior
   - loading states

Main commits:
- `5712f15` - `Document setup and deployment workflow`
- `7c5d3cf` - `feat: top nav tabs, inline modify, optimistic UI, loading states`

### Phase 6 - Vercel diagnosis

User ask:
- determine why Vercel deployment failed

What was found:
1. Vercel could not discover a FastAPI entrypoint because the FastAPI app lived under `mcp_server/server.py` instead of an auto-detected path
2. the local runnable app is not FastAPI at all; it is a custom `ThreadingHTTPServer`
3. Vercel is not a good fit for the current local app because the active runtime uses:
   - long-lived server process
   - local SQLite persistence
   - in-process scheduler thread

Key conclusion:
- the repo can be adapted for Vercel only after structural changes; it is currently better suited to a conventional host or AWS-oriented deployment

### Phase 7 - Non-secret implementation pass

User ask:
- complete everything possible without requiring live keys, specifically:
  - auth sessions / runtime events / Slack approvals / threshold overrides
  - observability and logging
  - dashboard/API auth
  - Slack approvals end-to-end in the local backend
  - real Google Ads write plumbing with preview/execute behavior
  - threshold tuning/calibration support
  - frontend updates as needed

What was done:
1. extended local configuration and SQLite schema for auth-related state, runtime events, Slack messages, and threshold overrides
2. added `backend/observability.py`
3. integrated structured runtime logging into server, actions, ads client, and orchestrator
4. added optional local dashboard/API auth
5. implemented signed Slack interactivity handling in the local backend
6. added Google Ads live write paths for:
   - campaign negative keywords
   - campaign pause
   - ad-group CPC bid adjustments for campaign-level bid actions
7. added validate-first behavior before real mutation execution
8. added threshold listing and local calibration support
9. exposed the new operational status/calibration data in the frontend
10. added/updated tests to keep the local path green

Main commit:
- `7a1809e` - `feat: add auth, observability, slack approvals, and live write plumbing`

### Phase 8 - Architecture/plan cross-reference review

User ask:
- cross reference the repository against the original docs and determine whether the overall plan was actually implemented

Verdict produced:
- partially implemented, not fully implemented

Reason:
- the repo contains a substantial amount of the planned architecture in code
- however, the active runnable runtime is still the local server + SQLite path
- production Secrets Manager/DynamoDB/S3/Object Lock behavior is not the current live system
- golden-account validation and final success gates are still incomplete
- several planned tools remain placeholders in the active local path

### Phase 9 - Repository cleanup for GitHub presentation

User ask:
- assess whether the README and file/folder layout follow normal GitHub standards and clean them up if not

What was done:
1. moved planning PDFs and extracts under `docs/planning/`
2. moved plain-text review extracts under `docs/reference/`
3. moved internal handoff/task-memory docs under `docs/project/`
4. moved Google Ads helper scripts under `scripts/google_ads/`
5. rewrote the root README into a shorter GitHub-facing entrypoint
6. added `docs/README.md` as a docs index
7. removed machine-specific absolute paths from the root README and internal docs where practical

Validation for this cleanup:
- `.venv/bin/pytest tests -q`
- result: `25 passed`

## Current Status Matrix

### Implemented enough to use locally
- local browser app
- local monitoring/orchestration loop
- account, alert, action, decision, report storage in SQLite
- core deterministic tools in the local path
- imported live Google Ads account reads
- local alert decision handling
- local dashboard/API auth
- local observability/runtime-event logging
- some real Google Ads write plumbing

### Partially implemented
- Slack end-to-end control plane
- Google Ads real-client pilot support
- production-oriented MCP server path
- threshold tuning/calibration
- campaign-builder flow
- production deployment model

### Still incomplete
- golden dataset calibration against real accounts
- live validation of Google Ads writes on a real client
- live Slack workspace validation for interactive approvals
- AWS deployment and runtime verification
- production persistence replacing local SQLite for hosted use
- full implementation of every planned tool in the local active path

## Real vs Demo Data

### Real-capable now
These can use real Google Ads data when credentials are present and an account is imported:
- connection test
- listing accessible customers
- importing a live customer into the local app
- campaign/account/search-term snapshot reads
- tool outputs derived from imported live snapshots
- selected live write actions

Relevant endpoints:
- `GET /api/google-ads/test`
- `GET /api/google-ads/customers`
- `POST /api/google-ads/import-account`

### Still mixed with demo/local state
- default account list before live import
- pre-seeded alerts and decisions
- seeded reports/history
- some recommendation/explanation flows
- much of the visible dashboard when using the seeded DB only

## Important Local State Notes

- The local SQLite database is at `data/ads_genie.db`.
- Do not casually reset or overwrite it without explicit user approval.
- The user introduced Google OAuth helper files and local credentials-related artifacts through another AI pass. Treat those as intentional unless the user asks to remove them.
- `client_secrets.json` must not be committed.

## Running The Current App

```bash
python3 -m backend.server
```

Default URL:
- `http://127.0.0.1:8080`

Caveat:
- agent-started background servers have sometimes looked healthy while remaining unreachable to the user browser; running directly in the user environment is more reliable

## Remaining Work

The current highest-value unfinished work is not more scaffolding. It is live validation.

Primary remaining tasks:
1. connect one real Google Ads client cleanly
2. isolate or reduce demo noise in the local DB for a trustworthy pilot
3. validate read-path outputs against the real Google Ads UI
4. test one safe write action end-to-end on a real client
5. validate Slack interactive approvals in a real workspace
6. tune thresholds against real account behavior
7. decide whether production deployment should target AWS-first instead of trying to force Vercel

## Next Task Note

Recommended owner:
- `any AI`

Exact next task:
- prepare a one-client live pilot by isolating demo noise, verifying credentials, importing one live Google Ads account, and validating the read path against the real Google Ads UI

Why this is next:
- the repo already has enough local functionality that more abstract scaffolding yields diminishing returns
- the biggest uncertainty is whether the implemented read/write behavior matches reality on a real account

Success criteria for the next task:
- one live account imported successfully
- dashboard/account metrics clearly traceable to real Google Ads data
- health check and budget-waste outputs manually spot-checked against Google Ads UI
- one low-risk write action tested with validate-first behavior
