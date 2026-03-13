# Tasks Left

Last updated: 2026-03-10

## Current Priority
- [x] Connect real Google Ads **read** data (OAuth + GAQL searchStream)
- [ ] Connect real Google Ads **write** actions (pause campaign, negatives, bid updates)

## Remaining Build Tasks
- [ ] Slack interactive approvals end-to-end (`Approve/Modify/Dismiss` callback handler)
- [ ] Dashboard/API authentication and authorization
- [ ] Golden dataset calibration against real accounts (threshold tuning)
- [ ] AWS deployment layer (Lambda, Step Functions, EventBridge, DynamoDB, Secrets)
- [ ] Production observability and error budgets (structured logs, alerts, dashboards)

## Notes
- Keep mixed account sources supported (`demo` + `live`).
- Never block monitoring loop on provider/API failures; fallback safely.
