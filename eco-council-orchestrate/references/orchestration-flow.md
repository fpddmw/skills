# Orchestration Flow

## Control Plane vs Data Plane

Keep these phases separate:

1. moderator review
   - mission is already fixed
   - moderator edits `tasks.json` only
2. expert raw collection
   - sociologist and environmentalist execute fetch commands
   - only `raw/` artifacts are written
3. deterministic data plane
   - normalize
   - link evidence
   - build round context
   - build report/decision drafts
4. expert and moderator deliberation
   - experts revise report drafts
   - moderator revises decision draft
5. promotion and next-round scaffolding
   - promote approved drafts
   - scaffold `round_002`, `round_003`, ...

## Recommended Loop

For each round:

1. `prepare-round`
2. expert agents run fetch prompts
3. `run-data-plane`
4. OpenClaw experts revise report drafts
5. OpenClaw moderator revises decision draft
6. `$eco-council-reporting promote-all`
7. if `council_decision.next_round_required=true`, run `advance-round`

## File Boundaries

- moderator input:
  - `mission.json`
  - `round_xxx/moderator/tasks.json`
- expert raw output:
  - `round_xxx/<role>/raw/*`
- deterministic exchange:
  - `round_xxx/shared/claims.json`
  - `round_xxx/shared/observations.json`
  - `round_xxx/shared/evidence_cards.json`
- report handoff:
  - `round_xxx/<role>/derived/*.json`
  - `round_xxx/<role>/derived/openclaw_*.txt`
