# Tasks Left

Last updated: 2026-03-17

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

## Remaining Build Tasks
- [ ] Validate live Google Ads write mutations against a real client account
- [ ] Validate Slack interactive approvals against a real Slack app/workspace
- [ ] Golden dataset calibration against real accounts (true threshold tuning)
- [ ] Decide and implement the real hosted production target
- [ ] Replace or complement SQLite with production-grade hosted persistence for deployment
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
- Cleanup validation: `.venv/bin/pytest tests -q` passed with `25 passed`.

## Next Task Note
- Recommended owner: `any AI`
- Next exact task: prepare a one-client live pilot by isolating demo noise, verifying credentials, importing one live Google Ads account, and validating the read path against the real Google Ads UI.
- Why this is next: the main unresolved risk is not missing scaffolding, it is whether the real account behavior matches the implemented local system.
- Validation target: one imported live account, real metric spot-checks, and one low-risk validate-first write test.
