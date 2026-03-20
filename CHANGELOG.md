# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project is not formally versioned yet, so milestone-style version labels are used to group major repository changes.

## [Unreleased]

### Added
- Added shared autonomy-level normalization in `orchestration/models/autonomy_levels.py` so legacy aliases such as `propose_wait` and `draft_review` are normalized consistently across the local app and MCP-style write paths.
- Added quiet-hours persistence and enforcement in the local account/orchestrator flow.
- Added local rollback support for supported executed actions, including rollback metadata tracking and rollback-result state transitions.
- Added campaign-draft persistence and APIs in the local backend for create, fetch, approve, and modify flows.
- Added campaign-builder prompt/context intake plus file-context upload handling in the frontend/local backend flow.
- Added `/api/notifications` and a top-right notification tray in the local frontend.
- Added tests covering autonomy alias normalization, quiet-hours deferral, rollback behavior, and campaign-draft/notification flows.

### Changed
- Tightened the local control-plane security posture with stricter auth/CORS behavior, safer frontend rendering, stricter MCP route access, improved runtime-event redaction, Slack replay protection, and pinned dependency versions.
- Updated the local UI/backend contract so more backend functionality is reachable from the browser, especially campaign-draft and notification flows.
- Expanded the SQLite-backed account model to persist quiet hours and normalized autonomy payloads.
- Updated handoff/task-memory/docs index files to reflect the latest security, autonomy, rollback, campaign-draft, and notification work.

### Fixed
- Fixed `enable_ad_group` autonomy-policy enforcement so it checks the correct action type.
- Fixed autonomy handling so legacy stored policy names are normalized to the current canonical levels.
- Fixed local orchestrator behavior so quiet hours defer auto-execute decisions into proposal/review behavior instead of executing immediately.
- Fixed local decision handling to allow rollback as an explicit alert decision path for supported actions.

### Security
- Hardened the main local app and MCP path against the first major round of auth, CORS, XSS, error-leakage, and replay issues.
- Documented the latest remaining security follow-ups after the newer UI/backend pass: object-level authorization and bounded request-body parsing still need a focused completion pass.

## [0.3.0] - 2026-03-17

### Added
- Added local dashboard/API authentication controls in the runnable backend using optional HTTP Basic Auth.
- Added runtime observability support with structured runtime events, request IDs, timing, error capture, and health-summary data.
- Added Slack interactivity handling in the local backend for signed `Approve`, `Modify`, and `Dismiss` actions.
- Added Slack message persistence for alert-to-message tracking in the local SQLite schema.
- Added Google Ads live write plumbing for:
  - campaign negative keywords
  - campaign pause
  - eligible ad-group CPC bid adjustments on campaign bid actions
- Added validate-first mutation behavior for live Google Ads writes before real execution.
- Added threshold override storage and threshold-calibration support from local account data.
- Added OAuth helper scripts for desktop credential generation and local Google Ads credential bootstrap.
- Added an `AI_HANDOFF.md` file to preserve project context for another AI agent.
- Added local API surfaces for health, runtime events, threshold inspection, and threshold calibration.

### Changed
- Expanded the SQLite schema to include runtime events, Slack messages, and threshold overrides.
- Updated the local orchestrator to log more state transitions and work with the new live-write and Slack-decision flows.
- Updated the server health response to include environment, scheduler, auth, credential, and runtime-summary details.
- Updated the frontend to expose more system status and operational feedback for auth/health/calibration flows.
- Updated `requirements.txt` to include dependencies needed by the improved local auth/OAuth path.
- Updated `.gitignore` to avoid committing sensitive local credential artifacts such as `client_secrets.json`.
- Updated `TASKS_LEFT.md` to reflect that auth, observability, local Slack approvals, and live write plumbing were implemented.

### Fixed
- Improved error handling around request dispatch so unexpected backend failures are logged and surfaced with request IDs.
- Improved action execution behavior so Google Ads failures are recorded instead of silently collapsing into generic local behavior.
- Improved resilience of the monitoring path by keeping fallback behavior when live provider calls fail.

### Security
- Prevented accidental source control inclusion of local Google OAuth client secret files.
- Added signature verification for Slack interactivity handling in the local backend path.

## [0.2.0] - 2026-03-12

### Added
- Added the command-center UI changes requested by the user-provided replacement frontend files:
  - top navigation tabs
  - inline modify workflow
  - optimistic alert UI updates
  - loading/disabled states for alert actions
- Added refreshed CSS and interaction behavior to align the local frontend with the later UI direction supplied by the user.

### Changed
- Replaced the existing local frontend files in `frontend/index.html`, `frontend/styles.css`, and `frontend/app.js` with the provided versions.
- Updated the demo SQLite state used by the local app after the UI replacement session.

## [0.1.1] - 2026-03-12

### Added
- Expanded `README.md` from a minimal stub into an operator/deployment guide covering:
  - local setup
  - running the app
  - tests
  - environment variables
  - Google Ads connection
  - local API endpoints
  - MCP server notes
  - Slack notes
  - AWS deployment direction
  - security notes
  - next recommended work

### Changed
- Improved repository documentation so the local app, scaffolded production path, and current limitations are explicitly documented.

## [0.1.0] - 2026-03-12

### Added
- Added the initial full Ads Genie application stack in this repository.
- Added a runnable local backend in `backend/` with:
  - HTTP server
  - SQLite database layer
  - seeded demo data
  - orchestration loop
  - reports
  - Slack bridge
  - deterministic analysis tools
- Added the initial local browser UI in `frontend/`.
- Added a live-capable Google Ads adapter supporting read-path connectivity and later-extensible write behavior.
- Added MCP-style server modules in `mcp_server/`.
- Added AWS-oriented orchestration, memory, report, and Slack control-plane scaffolds.
- Added CDK infrastructure scaffolds for database, MCP server, orchestration, and Slack bot stacks.
- Added unit and integration test coverage for the implemented core paths.
- Added `TASKS_LEFT.md` to preserve outstanding work.
- Added local architecture/implementation companion text files for the supplied planning documents.
- Added `.env.example`, dependency manifests, and repository ignore rules.

### Changed
- Established the repository as a combined local runnable app plus production-architecture scaffold rather than a docs-only prototype.

## [0.0.1] - 2026-03-12

### Added
- Initial repository bootstrap with a placeholder `README.md` before the main Ads Genie implementation was added.
