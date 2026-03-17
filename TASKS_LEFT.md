# Tasks Left

Last updated: 2026-03-17

## Current Priority
- [x] Connect real Google Ads **read** data (OAuth + GAQL searchStream)
- [x] Connect real Google Ads **write** actions (pause campaign, negatives, bid updates)

## Remaining Build Tasks
- [x] Slack interactive approvals end-to-end (`Approve/Modify/Dismiss` callback handler)
- [x] Dashboard/API authentication and authorization
- [x] Threshold override / local calibration support
- [x] Production-style observability pass for the local app (structured runtime logs, events, health summary)
- [ ] Validate live Google Ads write mutations against a real client account
- [ ] Validate Slack interactive approvals against a real Slack app/workspace
- [ ] Golden dataset calibration against real accounts (true threshold tuning)
- [ ] AWS deployment layer (Lambda, Step Functions, EventBridge, DynamoDB, Secrets)
- [ ] Hosted deployment target and non-SQLite production persistence

## Notes
- Keep mixed account sources supported (`demo` + `live`).
- Never block monitoring loop on provider/API failures; fallback safely.
- `client_secrets.json` is intentionally ignored and must not be committed.
