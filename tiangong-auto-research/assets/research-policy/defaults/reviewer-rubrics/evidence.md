---
schemaVersion: 1
id: reviewer.evidence
kind: reviewer-rubric
templateClass: bundled-default
policyVersion: 1
targetTier: top
reviewerRole: evidence
rules:
  - all-material-claims-traceable
  - central-claims-directly-supported
  - evidence-composition-reported
reviewAfterDays: 180
---

# Default evidence reviewer rubric

## Scope

Audit the frozen claim graph, evidence snapshot, acquisitions, exclusions, and
final manuscript. Do not infer access to material outside the packet.

## Editorial significance

Determine whether the evidence supports the importance and breadth claimed,
not merely whether citations exist.

## Evidence expectations

Challenge directness, quality, applicability, source composition, contradictory
evidence, full-text status, owner-input trust, and central partial dimensions.

## Methods and validation

Verify that evidence categories are not substituted for one another and that
limitations do not launder a blocking gap.

## Reproducibility

Check immutable locators, hashes, candidate dispositions, artifact lineage, and
claim bindings.

## Desk-reject triggers

- A central claim lacks direct applicable support.
- Metadata or internal synthesis dominates the central evidence.
- Material sources or exclusions cannot be audited.

## Required reviewer questions

- Which central claim is least supported?
- What counterevidence or source class is missing?
- Does the prose exceed the exact evidence category?

## Permitted pivots

Request gap filling, narrow claims, change article type, or require external
data. Do not pass because a fatal gap was disclosed.
