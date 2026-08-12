---
schemaVersion: 1
id: article.methods-data-resource
kind: article-type
templateClass: bundled-default
policyVersion: 1
targetTier: top
articleType: methods-data-resource
rules:
  - benchmark-comparison-required
  - reuse-value-demonstrated
  - availability-contract-required
  - material-results-reproduced
requiredReviewers:
  - evidence
  - methods-reproducibility
  - domain-novelty
  - journal-editor
reviewAfterDays: 180
---

# Default methods, data, and resource policy

## Scope

Use when the primary contribution is a reusable method, dataset, benchmark,
software system, protocol, or community resource.

## Editorial significance

Require broad enabling value, a clear unmet need, and credible adoption or
scientific impact beyond the authors' immediate application.

## Evidence expectations

Document provenance, coverage, missingness, bias, quality controls, licensing,
and intended and prohibited uses of the resource.

## Methods and validation

Compare with strong baselines on representative tasks, report failure modes,
and demonstrate reuse by an independent workflow or held-out application.

## Reproducibility

Provide exact versioned artifacts, schemas, environments, examples, and a
verified path from raw inputs to released outputs.

## Desk-reject triggers

- The resource is unavailable, unstable, or insufficiently documented.
- Evaluation uses weak baselines or only the construction dataset.
- The contribution is engineering scale without a scientific advance.

## Required reviewer questions

- Can an independent group reproduce and reuse the contribution?
- Does the benchmark measure the claimed capability without leakage?
- Are licensing, governance, and maintenance credible?

## Permitted pivots

Release a validated resource first, narrow the application claim, add an
independent benchmark, or target a specialist methods or data journal.
