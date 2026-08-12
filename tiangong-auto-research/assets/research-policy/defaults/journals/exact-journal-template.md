---
schemaVersion: 1
id: journal.exact-template
kind: exact-journal
templateClass: exact-journal-template
policyVersion: 1
targetTier: top
journalName: __REPLACE_JOURNAL_NAME__
officialGuidelinesUrl: __REPLACE_OFFICIAL_HTTPS_URL__
officialGuidelinesRetrievedAt: __REPLACE_YYYY-MM-DD__
rules:
  - exact-journal-scope-confirmed
  - exact-journal-article-type-confirmed
  - exact-journal-guidelines-verified
requiredReviewers:
  - journal-editor
reviewAfterDays: 90
---

# Exact target-journal policy template

Replace every placeholder using current official journal sources. Agent-drafted
content requires explicit human approval and remains invalid while any
placeholder is present.

## Scope

Describe the exact journal scope, audience, accepted article type, and why this
project belongs there.

## Editorial significance

Define what this journal's editors are likely to regard as a sufficiently
important and non-incremental contribution, supported by current official
guidance and a bounded sample of recent representative articles.

## Evidence expectations

Record journal-specific evidence, data availability, reporting, ethics, and
supplement requirements.

## Methods and validation

Record journal-specific study design, validation, statistics, and review
expectations without weakening the baseline or article-type policy.

## Reproducibility

Record exact data, code, materials, repository, and availability requirements.

## Desk-reject triggers

List scope, article-type, significance, methods, length, presentation, and
submission defects that would prevent external review.

## Required reviewer questions

List the questions a fresh editor for this exact journal should answer about the
final frozen manuscript.

## Permitted pivots

List acceptable journal-class, article-type, additional-research, or external
handoff routes when the exact target cannot be supported.
