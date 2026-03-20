# Docs Index

This repository keeps long-form supporting material under `docs/` so the repo root stays focused on the application itself.

## Structure

- `docs/planning/`
  - original architecture and implementation planning documents
- `docs/reference/`
  - extracted/plain-text companion files used for local review and AI comparison work
- `docs/project/`
  - live project memory, handoff notes, and next-task tracking
  - includes the current implementation/reality notes after recent security, autonomy, rollback, and UI comparison passes

## Start Here

- [Project handoff](project/AI_HANDOFF.md)
- [Task memory](project/TASKS_LEFT.md)
- [Root changelog](../CHANGELOG.md)

Use those three files together.
They are the canonical record of:
- what is implemented
- what was validated
- what remains blocked
- what the next AI should do next

## Planning Documents

- [Architecture review PDF](planning/Ads-Genie-Architecture-Review-and-Strategic-Recommendations.pdf)
- [Implementation plan PDF](planning/Ads-Genie-Implementation-Plan.pdf)
- [Google API design DOCX](planning/AdsGenie-Google-API-Design-Documentation.docx)

## Reference Extracts

- [Architecture extract](reference/architecture.txt)
- [Implementation extract](reference/implementation.txt)

## Current Reality

- The active runtime remains the local `backend/` + `frontend/` app.
- The repo also contains a production-oriented scaffold under `mcp_server/`, `orchestration/`, `slack_bot/`, and `infrastructure/`.
- Recent project-memory updates include:
  - security hardening and follow-up security review notes
  - autonomy normalization, quiet hours, and rollback support
  - campaign-draft, file-context, and notification-tray UI/backend work
  - explicit notes on what is still not spec-complete from the latest UI audit
