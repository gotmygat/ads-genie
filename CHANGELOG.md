# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project is not formally versioned yet, so milestone-style version labels are used to group major repository changes.

## [Unreleased]

### Added
- Added this `CHANGELOG.md` file to document the repository history from inception in a Keep a Changelog-style format.
- Added a documentation-maintenance rule to the handoff/task-memory flow so future work updates the changelog, handoff, and next-task note together.
- Added a consolidated implementation-history summary to the AI handoff document so another agent can continue without replaying the full thread.
- Added `docs/README.md` as a documentation index so planning material, extracts, and project-memory files are grouped under a standard docs folder.

### Changed
- Reorganized root-level repository clutter into `docs/` and `scripts/` to make the GitHub file tree look more like a conventional project repository.
- Moved handoff/task-memory files from the repo root into `docs/project/`.
- Moved planning PDFs and extracted planning/reference text into `docs/planning/` and `docs/reference/`.
- Moved Google Ads OAuth helper scripts into `scripts/google_ads/`.
- Rewrote the root `README.md` into a shorter GitHub-facing overview with links to deeper documentation.
- Updated the handoff documentation to reflect the current reality of the codebase: a working local app plus a partially implemented production scaffold.
- Updated task memory to include an explicit end-of-task note describing what to work on next.
- Validated the repository cleanup with the existing test suite (`25 passed`).

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
