# Top-journal Research Policy and final publication gate

Load this reference for a top-journal paper, target-journal readiness claim,
final manuscript review, or publication closure. The goal is a truthful,
reviewable submission candidate; no policy or reviewer can guarantee acceptance.

## Initialize Policy before project admission

Run the guided Policy Wizard only after project-scoped setup is `READY` and the
installed orchestrator matches the immutable setup plan:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy wizard PROJECT --workspace /absolute/path/to/workspace
```

The Wizard loads only that verified project installation. It selects one
article type, field, and journal class, copies conservative Markdown defaults,
completes the publication brief, and optionally creates an exact-journal
policy. It always warns that defaults are generic. Approval requires a separate
acknowledgement and binds the exact current content hash.

Use deterministic commands for automation or audit:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy catalog --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy status PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research policy approve PROJECT --confirm --acknowledge-defaults \
  --workspace /absolute/path/to/workspace --json
```

Do not hand-copy a policy pack or use an ambient/global Skill as an implicit
source. `--source-root` is an explicit automation escape hatch and remains
subject to non-symlink validation; the Wizard never uses it.

## Human-maintained Markdown

The editable project stack is `research-policy/PROJECT/`. A human may update:

- `publication-brief.md`: central question, claim, outcome, contribution, and
  evidence, method, novelty, deliverable, stop, and handoff conditions;
- `article-type.md`, `field.md`, and `journal-class.md`: stricter applicable
  requirements for this study;
- `journal.md`, when present: current official scope, editorial threshold,
  evidence, methods, reproducibility, desk-reject, reviewer, and pivot rules;
- reviewer rubrics for evidence, methods/reproducibility, domain/novelty, and
  the journal editor.

An exact-journal policy cannot be approved by changing only its header. Name
the journal, cite a current official HTTPS guideline URL and retrieval date,
and replace every substantive generic section. Never infer journal rules from
memory or weaken the baseline with a less strict local rule.

Status is mechanical: `missing`, `default-unapproved`, `custom-draft`,
`default-approved`, `custom-approved`, `conflict`, `stale`, `changed`, or
`invalid`. Any content edit after approval yields `changed`; expiry yields
`stale`. Both stop project stages and publication review until re-approved.

## Verdict ceilings

Defaults support feasibility and planning but are not journal endorsement. The
CLI computes the maximum permitted language:

- generic stack: `top-journal-candidate`;
- human-customized article, field, journal class, and project brief:
  `top-journal-class-ready`;
- the above plus a complete exact-journal policy:
  `target-journal-submission-ready`.

Mechanical evidence or review failures can only lower the result. An editor
cannot raise a ceiling or override an unobserved central outcome, incomplete
recall, missing direct evidence, or unreproduced result.

## Author in the current native host

After the base `discover → acquire → analyze → synthesize → review → close`
project is complete, write the final manuscript and assessment in the current
Codex app/session or interactive Claude Code session. The CLI remains a control
plane and must not launch a nested producer. Inspect the schema, then freeze:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show publication-assessment --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication freeze PROJECT \
  --manuscript /absolute/path/to/final-manuscript.md \
  --assessment /absolute/path/to/publication-assessment.json \
  --producer-agent codex --producer-session OPAQUE_NATIVE_SESSION \
  --workspace /absolute/path/to/workspace --json
```

Freeze content-addresses the manuscript, assessment, supplements, approved
Policy, final evidence snapshot, and base outputs. It evaluates central claims,
outcomes, result classes, evidence composition, owner-input trust, recall,
novelty, reproduction, and pivots. File existence alone is never success.

## Four fresh independent final reviews

Prepare and submit exactly one review for each role: `evidence`,
`methods-reproducibility`, `domain-novelty`, and `journal-editor`.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication review prepare PROJECT --role evidence \
  --reviewer-agent claude --reviewer-session FRESH_OPAQUE_SESSION \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research schema show publication-review-evidence --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication review submit PROJECT --role evidence \
  --review /absolute/path/to/review.json \
  --workspace /absolute/path/to/workspace --json
```

Repeat with fresh sessions. A reviewer may use the configured headless Codex or
Claude CLI, but must not be the producer session or any prior project reviewer
session. The append-only journal stores only its SHA-256 for reuse detection;
deleting a mutable cache does not permit reuse.

Each packet binds the exact Policy, evidence snapshot, base closure, manuscript,
assessment, supplements, mechanical result, role, reviewer, and schema. Review
cannot add evidence. A manuscript revision creates a new generation and
invalidates every old review; a Policy change or expiry blocks publication.

## Close and report bounded language

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication status PROJECT --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research publication close PROJECT --workspace /absolute/path/to/workspace --json
```

Closure re-verifies every hash and requires all four reviews. Report only the
returned verdict, bounded statement, limitations, and pivots.
`target-journal-submission-ready` means the frozen artifact passed its declared
gates; it does not predict or guarantee editorial acceptance.
