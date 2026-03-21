# Eco Council Reporting OpenClaw Templates

## Template A: Expert Report Drafting

Use when `report_packet.json` already exists and the sociologist or environmentalist should produce one `expert-report`.

### Sociologist

```text
Use $eco-council-reporting.
Open and follow:
[RUN_DIR]/round_001/sociologist/derived/openclaw_report_prompt.txt
Return only JSON.
```

### Environmentalist

```text
Use $eco-council-reporting.
Open and follow:
[RUN_DIR]/round_001/environmentalist/derived/openclaw_report_prompt.txt
Return only JSON.
```

## Template B: Moderator Decision Drafting

Use when expert-report drafts already exist and the moderator should decide whether to continue, complete, or block the round.

```text
Use $eco-council-reporting.
Open and follow:
[RUN_DIR]/round_001/moderator/derived/openclaw_decision_prompt.txt
Return only JSON.
```

## Template C: Promote Approved Drafts

Use after the returned JSON has been reviewed and written to the draft paths referenced by the prompt files.

### Promote everything

```text
Use $eco-council-reporting.
Run:
python3 scripts/eco_council_reporting.py promote-all \
  --run-dir [RUN_DIR] \
  --round-id round-001 \
  --pretty
Return only JSON.
```

### Promote one report only

```text
Use $eco-council-reporting.
Run:
python3 scripts/eco_council_reporting.py promote-report-draft \
  --run-dir [RUN_DIR] \
  --round-id round-001 \
  --role sociologist \
  --pretty
Return only JSON.
```

### Promote one moderator decision only

```text
Use $eco-council-reporting.
Run:
python3 scripts/eco_council_reporting.py promote-decision-draft \
  --run-dir [RUN_DIR] \
  --round-id round-001 \
  --pretty
Return only JSON.
```

## Orchestration Rules

- Build packets before rendering prompts.
- Let expert agents revise only draft objects, not canonical outputs.
- Promote drafts only after validation and review.
- Rebuild packets for a round before asking the moderator for a new decision if upstream canonical objects changed.
