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

Latest relevant commit before this handoff/publish pass:
- `e2146b0` - `Harden auth, CORS, and XSS protections across app and MCP`

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

### Phase 10 - Stop tracking the local SQLite database

User ask:
- remove the tracked SQLite DB from version control and explain what that means

What was done:
1. added `data/*.db` to `.gitignore`
2. removed `data/ads_genie.db` from Git tracking while leaving the local file usable on disk
3. kept the runtime bootstrap model based on `Database.init_schema()` and `seed_demo_data()`
4. documented that the SQLite DB is generated local state, not a source artifact
5. retained the `data/` directory in the repo via `data/.gitkeep`

### Phase 11 - Security hardening pass

User ask:
- find and patch the current repo security mistakes, day-0 vulnerabilities, and obvious app/MCP security flaws

What was done:
1. tightened the local app CORS/origin posture instead of allowing broad default access
2. added stricter auth requirements around the local control plane depending on runtime configuration
3. reduced frontend XSS risk by removing unsafe rendering paths and sanitizing UI-bound content
4. gated MCP routes behind bearer-token auth and disabled open docs exposure by default
5. redacted more sensitive runtime-event payload data and reduced error-detail leakage
6. added Slack replay-signature caching
7. fixed an autonomy-policy bug in `enable_ad_group`
8. stopped printing the full OAuth refresh token in helper-script stdout
9. pinned runtime/dev dependencies instead of leaving broad floating ranges

Validation for this phase:
- `.venv/bin/pytest tests -q`
- result reported during that pass: `26 passed`

Main commit:
- `e2146b0` - `Harden auth, CORS, and XSS protections across app and MCP`

### Phase 12 - Plan/doc reconciliation implementation pass

User ask:
- read the supplied planning documents, compare them against the repo, and implement the missing work that was still practical in the current local path

What was done:
1. read the supplied DOCX/PDF planning documents fully
2. produced a repo-vs-plan gap report
3. normalized legacy autonomy aliases through a shared `orchestration/models/autonomy_levels.py`
4. persisted and enforced account quiet hours in the local backend/orchestrator
5. added rollback handling for previously executed local actions where rollback metadata exists
6. updated write-action policy helpers to use the normalized autonomy model consistently
7. added tests covering autonomy normalization, quiet-hours deferral, rollback behavior, and related write-action behavior

Validation reported during that pass:
- targeted suite: `16 passed`
- full suite: `31 passed`

Important limitation:
- rollback is still local-path oriented; live Google Ads rollback is not generally implemented

### Phase 13 - UI/spec2 implementation pass

User ask:
- compare the frontend against `ads-genie-ui-spec2.md` and make sure backend capabilities are actually reachable from the UI where it makes sense

What was done:
1. added a campaign-builder intake flow with:
   - freeform prompt/context input
   - context-file upload support for the AI/campaign draft flow
   - persisted campaign draft records in SQLite
2. added campaign-draft APIs in the local backend, including:
   - create draft
   - fetch latest draft
   - fetch one draft
   - approve draft
   - modify draft
3. added `/api/notifications` and a top-right notification tray in the frontend
4. added frontend routing/state support for campaign-draft and alerts-oriented surfaces
5. connected the new UI elements to the local backend endpoints so the local app exposes more of the implemented backend behavior

Validation note:
- a later local rerun of `tests/test_system.py` in the agent sandbox hit a socket-bind permission error while starting a temporary `ThreadingHTTPServer`
- this looked environment-related rather than a deterministic app regression, but it means browser/runtime verification is still recommended from the user environment

Important reality check from the follow-up UI audit:
- improved, but not spec-complete
- remaining spec2 gaps include:
  - replace remaining `/decision`-style alert flow assumptions with the approval/modify/dismiss contract expected by the spec
  - replace any prompt-based modify flow with an inline editor everywhere it still remains
  - finish account-scoped alert/status/settings/query/log surfaces expected by the spec
  - complete the alert polling/new-alert banner behavior expected by the spec
  - decide whether strict spec compliance requires a React/Tailwind/router-state rewrite instead of the current vanilla frontend

### Phase 14 - Final security review after the newer backend/UI work

User ask:
- run one more security sweep against the updated repo

Latest review outcome:
- no new critical findings were reported
- highest remaining risks were:
  - missing object-level authorization on some account/draft/alert actions
  - unbounded POST body reads that could allow memory-pressure abuse
- medium-risk follow-ups remained around:
  - CSRF/origin edge cases
  - DOCX/XML parsing/resource-exhaustion risk
  - some DOM XSS exposure in newer notification/draft UI paths
  - approval/decision race/idempotency handling
  - internal error detail leakage

Current interpretation:
- the repo is materially harder to exploit than before `e2146b0`
- however, the latest UI/back-end additions still need one more focused security pass before claiming the local control plane is fully hardened

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
- normalized autonomy policy handling across local and MCP-style write paths
- quiet-hours deferral in the local orchestrator
- local rollback support for supported executed actions
- campaign draft creation/review APIs and persisted context files
- top-right notification tray backed by `/api/notifications`

### Partially implemented
- Slack end-to-end control plane
- Google Ads real-client pilot support
- production-oriented MCP server path
- threshold tuning/calibration
- campaign-builder flow
- UI/spec2 alignment
- production deployment model

### Still incomplete
- golden dataset calibration against real accounts
- live validation of Google Ads writes on a real client
- live Slack workspace validation for interactive approvals
- AWS deployment and runtime verification
- production persistence replacing local SQLite for hosted use
- full implementation of every planned tool in the local active path
- full spec2 frontend/API contract compliance
- final hardening of object-level authorization and large-body handling in the newest endpoints

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
- `data/ads_genie.db` is no longer intended to be tracked in Git; treat it as generated local state.
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
1. patch the highest remaining security findings on the new account/draft/alert surfaces
2. finish the missing spec2 alert/status/settings/query/log contract work in the local app
3. connect one real Google Ads client cleanly
4. isolate or reduce demo noise in the local DB for a trustworthy pilot
5. validate read-path outputs against the real Google Ads UI
6. test one safe write action end-to-end on a real client
7. validate Slack interactive approvals in a real workspace
8. tune thresholds against real account behavior
9. decide whether production deployment should target AWS-first instead of trying to force Vercel

## Next Task Note

Recommended owner:
- `any AI`

Exact next task:
- close the current highest-risk gaps by hardening object-level authorization and request-size limits, then finish the missing spec2 local API/UI contract for alerts, status, settings, query/log, and approval-only execution behavior

Why this is next:
- the repo now has materially more UI/backend surface area, so the immediate risk is mismatch and exposure on the newly added endpoints
- after that hardening/contract pass, the next highest-value work returns to real-client validation instead of more speculative architecture work

Success criteria for the next task:
- account/draft/alert mutations are protected by object-level authorization and bounded request parsing
- local UI uses the intended alert decision/status contract without prompt-based fallbacks
- missing spec2-facing endpoints or UI surfaces are either implemented or explicitly documented as deferred
- targeted tests for the new security/UI contract pass, followed by a fresh user-environment runtime check
