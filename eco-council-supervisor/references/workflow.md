# Supervisor Workflow

The supervisor keeps a strict split:

1. Local shell stages
   - `prepare-round`
   - `execute-fetch-plan`
   - `run-data-plane`
   - `promote-all`
   - `advance-round`
2. Agent stages
   - moderator task review
   - sociologist report draft
   - environmentalist report draft
   - moderator decision draft

## Files the user should care about

- `RUN_DIR/supervisor/CURRENT_STEP.txt`
- `RUN_DIR/supervisor/state.json`
- `RUN_DIR/supervisor/sessions/*.txt`
- `RUN_DIR/supervisor/outbox/*.txt`

## Stage Map

- `awaiting-moderator-task-review`
  - Send the moderator session prompt and task-review outbox prompt.
  - Import the returned JSON with `import-task-review`.
- `ready-to-prepare-round`
  - Run `continue-run`.
- `ready-to-execute-fetch-plan`
  - Run `continue-run`.
- `ready-to-run-data-plane`
  - Run `continue-run`.
- `awaiting-expert-reports`
  - Send the two expert outbox prompts.
  - Import each JSON with `import-report`.
- `awaiting-moderator-decision`
  - Send the moderator decision outbox prompt.
  - Import the JSON with `import-decision`.
- `ready-to-promote`
  - Run `continue-run`.
- `ready-to-advance-round`
  - Run `continue-run`.
- `completed`
  - Stop.

## OpenClaw Note

`provision-openclaw-agents` creates isolated OpenClaw agents, but it does not force a chat channel. That keeps Feishu optional. You can talk to the agents through whatever OpenClaw surface you prefer.
