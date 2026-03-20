# Tasks Left

Last updated: 2026-03-20

## Documentation Maintenance Rule

After every completed task, update all of these files together:
- `CHANGELOG.md`
- `docs/project/AI_HANDOFF.md`
- `docs/project/TASKS_LEFT.md`

Each update must record:
- what changed
- what was validated
- what remains blocked
- the next recommended task
- who should pick it up next: `same AI` or `any AI`

## Current Priority
- [x] Connect real Google Ads read data (OAuth + Google Ads searchStream)
- [x] Connect real Google Ads write actions (campaign negatives, pause, bid adjustments)
- [x] Add local dashboard/API auth
- [x] Add local observability/runtime logging
- [x] Add local Slack interactive approvals
- [x] Add threshold override and local calibration support
- [x] Clean up repository layout and shorten the GitHub-facing README
- [x] Stop tracking the generated local SQLite database in Git
- [x] Harden the main local app and MCP path against the first major round of auth/CORS/XSS/security issues
- [x] Normalize legacy autonomy aliases and enforce quiet-hours deferral in the local orchestrator
- [x] Add local rollback support for supported executed actions
- [x] Add campaign-draft persistence, prompt/file-context intake, and notification tray support in the local UI/backend

## Remaining Build Tasks
- [ ] Patch the latest high-severity security findings on the new account/draft/alert surfaces:
  - object-level authorization
  - bounded request-body parsing
- [ ] Validate live Google Ads write mutations against a real client account
- [ ] Validate Slack interactive approvals against a real Slack app/workspace
- [ ] Golden dataset calibration against real accounts (true threshold tuning)
- [ ] Decide and implement the real hosted production target
- [ ] Replace or complement SQLite with production-grade hosted persistence for deployment
- [ ] Finish the remaining spec2-alignment work in the active local app path:
  - account-scoped alert/status/settings/query/log surfaces where required
  - approval/modify/dismiss alert contract cleanup
  - remove any remaining prompt-style modify flow
  - confirm approval-only behavior where the spec disallows auto-execute
  - finalize notification polling/banner behavior
- [ ] Complete the remaining placeholder tools in the active app path (`competitor_analysis`, `landing_page_audit`, `keyword_expansion`, `ad_copy_performance`, `pacing`)
- [ ] Prove the production scaffold end-to-end if AWS remains the target (Lambda, Step Functions, EventBridge, DynamoDB, Secrets)

## Constraints And Notes
- Keep mixed account sources supported (`demo` + `live`).
- Never block the monitoring loop on provider/API failures; fallback safely.
- `client_secrets.json` is intentionally ignored and must not be committed.
- Do not casually reset `data/ads_genie.db` without user approval.
- `data/ads_genie.db` is generated local state and should remain untracked.
- The repo contains both a local runnable system and a separate production-oriented scaffold; do not confuse the two when reporting status.
- Root-level clutter was reduced by moving support material into `docs/` and `scripts/`; keep new non-runtime material out of the repo root unless it is a standard root file.
- Last full-suite result reported before the current publish pass: `.venv/bin/pytest tests -q` passed with `31 passed`.
- Latest sandbox rerun note: `tests/test_system.py` hit a local socket-bind permission failure when starting a temporary `ThreadingHTTPServer`; treat that as an environment-specific blocker until verified outside the agent sandbox.
- The recent UI/spec2 pass improved campaign-building, draft review, and notifications, but an explicit follow-up audit still marked the frontend/backend contract as not fully spec-complete.
- The latest follow-up security review reported no critical issues, but it did leave two top-priority fixes open: object-level authorization and unbounded body reads on the newest endpoints.

## Next Task Note
- Recommended owner: `any AI`
- Next exact task: harden the new account/draft/alert surfaces by adding object-level authorization and bounded request parsing, then finish the missing spec2 contract work for alerts/status/settings/query/log and approval-only UI behavior.
- Why this is next: the highest immediate risk is that newer UI/backend surfaces are only partially aligned to the intended contract and still carry the latest security-review findings.
- Validation target: targeted security/UI tests for those endpoints pass, the local frontend uses the intended approval workflow without prompt fallbacks, and a user-environment runtime check confirms the updated paths behave correctly.
