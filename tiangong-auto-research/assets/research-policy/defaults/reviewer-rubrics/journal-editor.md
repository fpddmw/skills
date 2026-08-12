---
schemaVersion: 1
id: reviewer.journal-editor
kind: reviewer-rubric
templateClass: bundled-default
policyVersion: 1
targetTier: top
reviewerRole: journal-editor
rules:
  - final-frozen-manuscript-required
  - target-fit-reviewed
  - readiness-language-bounded
reviewAfterDays: 90
---

# Default target-journal editor rubric

## Scope

Review only the final frozen manuscript, figures, tables, supplement, policy
stack, and preceding independent verdicts. Simulate editorial triage; do not
upgrade unresolved scientific defects.

## Editorial significance

Judge whether the contribution, timing, audience, clarity, and scope merit
external review at the exact target journal.

## Evidence expectations

Check that title, abstract, highlights, figures, conclusions, and significance
language remain within the frozen evidence and result classifications.

## Methods and validation

Determine whether an editor can reasonably trust the design and whether obvious
reviewer objections have been answered before submission.

## Reproducibility

Verify that the exact manuscript and supplement hashes match the reviewed
packet and that availability statements point to bound artifacts.

## Desk-reject triggers

- Scope or article type does not match the journal.
- The central contribution is incremental or unsupported.
- Any blocking reviewer verdict remains unresolved.
- The manuscript changed after its final review.

## Required reviewer questions

- Would this exact manuscript be sent to external review?
- What single reason would most likely cause desk rejection?
- Does every user-facing readiness statement match the strictest verdict?

## Permitted pivots

Return desk reject, redesign, major revision, minor revision, or submission
ready. Recommend another article type or journal only as an explicit pivot.
