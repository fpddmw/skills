---
schemaVersion: 1
id: article.systematic-review-meta-analysis
kind: article-type
templateClass: bundled-default
policyVersion: 1
targetTier: top
articleType: systematic-review-meta-analysis
rules:
  - protocol-required
  - complete-candidate-disposition-required
  - recall-audit-required
  - risk-of-bias-required
  - synthesis-method-justified
requiredReviewers:
  - evidence
  - methods-reproducibility
  - domain-novelty
  - journal-editor
reviewAfterDays: 180
---

# Default systematic review and meta-analysis policy

## Scope

Use for systematic reviews, scoping reviews that claim systematic coverage, and
quantitative evidence synthesis.

## Editorial significance

Require a decision-relevant unresolved question, a defensible synthesis beyond
listing studies, and a clear explanation of why an updated review matters.

## Evidence expectations

Define databases, date bounds, query families, eligibility criteria, duplicate
handling, backward and forward citation search, and a disposition for every
candidate. Metadata-only records cannot substitute for required full text.

## Methods and validation

Use an applicable reporting guideline, protocol, risk-of-bias assessment,
heterogeneity analysis, sensitivity checks, and justified synthesis model.

## Reproducibility

Freeze all queries, dates, result sets, screening decisions, exclusion reasons,
extractions, transformations, and analysis code.

## Desk-reject triggers

- Unassessed candidates remain when completeness is claimed.
- Core databases or known landmark studies are missing.
- Search stops at a source-count minimum rather than saturation and recall.
- Heterogeneous evidence is pooled without a defensible model.

## Required reviewer questions

- Which omitted database, query synonym, or citation chain could change recall?
- Are exclusion reasons complete and reproducible?
- Is risk of bias propagated into the main conclusion?
- Does the synthesis add knowledge beyond prior reviews?

## Permitted pivots

Narrow the review question, label the work as a scoping or narrative review,
extend screening, or publish a protocol while evidence collection continues.
