---
schemaVersion: 1
id: baseline.top-journal
kind: baseline
templateClass: bundled-default
policyVersion: 1
targetTier: top
rules:
  - central-claim-directly-supported
  - contribution-non-incremental
  - material-results-reproduced
  - alternative-explanations-tested
  - final-manuscript-reviewed
constraints:
  minDirectPeerReviewedFullText: 3
  requireRecallAudit: true
  requireCentralDimensionsCovered: true
  requireIndependentReproduction: true
  requireScientificDesignContract: true
  requireEarlyScientificReviews: true
  requireRealRecordConstructCanary: true
requiredReviewers:
  - evidence
  - methods-reproducibility
  - domain-novelty
  - journal-editor
reviewAfterDays: 180
---

# Default top-journal baseline

This is a conservative generic default. It supports feasibility research but is
not a journal-specific endorsement and cannot by itself establish submission
readiness.

## Scope

Apply this baseline when the intended contribution targets a highly selective
general or disciplinary journal. Combine it with one article-type policy, one
field policy, one journal-class or exact-journal policy, and a project brief.

## Editorial significance

Require a consequential, timely, and non-incremental contribution whose value
is legible beyond the immediate dataset or case. A technically correct result
without a credible importance argument does not pass.

## Evidence expectations

Bind every central claim to direct, applicable evidence. Separate direct
peer-reviewed full text from metadata, background, administrative material,
owner input, and internal synthesis. A central partial or missing dimension is
blocking for an original or systematic claim.

## Methods and validation

Require a research design capable of answering the central question, explicit
assumptions, tested alternative explanations, uncertainty analysis, and
validation appropriate to the claimed scope. Disclosed fatal gaps remain fatal.
Freeze endpoint truth roles, quantity/denominator scope, original and independent
units, resampling units, threshold classes, and baseline fairness before result
inspection. Require a real-record, outcome-blind construct canary before the
main acquisition/analysis budget is spent.

## Reproducibility

Bind data, code, parameters, commands, tables, figures, and derived statistics
to exact artifacts. Do not treat a provenance description or hard-coded value
as computational reproduction.

## Desk-reject triggers

- The central result is an identity, illustrative sensitivity, or agenda item
  presented as an empirical discovery.
- The title or abstract promises an outcome that was not observed, calibrated,
  or validated.
- The contribution is incremental, outside scope, or unsupported by direct
  evidence.
- Synthetic examples stand in for a feasible real-record join, repeated rows are
  counted as independent units, or one model is called ground truth without an
  independently observed endpoint.
- The final manuscript was created or materially changed after review.

## Required reviewer questions

- What is genuinely new, and which closest prior work could defeat that claim?
- Which central claim has the weakest direct evidence or validation?
- What plausible alternative explanation remains untested?
- Would a target editor send this exact frozen manuscript to external review?

## Permitted pivots

Pivot to a Perspective, evidence report, research protocol, additional data or
experiments, a different journal class, or an external-response handoff. Never
preserve a top-journal verdict by relabeling a blocking gap as a limitation.
