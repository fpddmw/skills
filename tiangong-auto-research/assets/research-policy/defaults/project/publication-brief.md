---
schemaVersion: 1
id: project.publication-brief-template
kind: publication-brief
templateClass: project-template
policyVersion: 1
targetTier: top
articleType: __SELECT_ARTICLE_TYPE__
field: __SELECT_FIELD__
journalClass: __SELECT_JOURNAL_CLASS__
targetJournal: __SELECT_EXACT_JOURNAL_OR_NONE__
centralQuestion: __DEFINE_CENTRAL_QUESTION__
centralClaim: __DEFINE_CENTRAL_CLAIM__
contributionType: __DEFINE_CONTRIBUTION_TYPE__
centralOutcome: __DEFINE_CENTRAL_OUTCOME__
rules:
  - project-brief-complete
reviewAfterDays: 365
---

# Project publication brief

Replace every placeholder. The current native Codex or Claude host may draft
this brief, but top-journal execution requires explicit human approval of the
exact hash.

## Contribution hypothesis

Explain what is genuinely new and why the intended readership should care.

## Central claim and result

Define the central claim, the result that would support it, its units and scope,
and the result class: observed, causal, calibrated, validated prediction,
illustrative sensitivity, identity, conceptual proposition, or future work.

## Required evidence

Define direct evidence, source composition, full-text, date, database, data,
counterevidence, and applicability requirements for each central claim.

## Research design and validation

Define data, methods, controls, baselines, alternatives, uncertainty,
robustness, external validation, and computational reproduction.

## Scientific identity, claims, and quantities

Define the central study kind and every supporting component; the estimand;
claim/evidence edges; endpoint truth roles and compatible comparisons; units,
quantities, denominators, allowed terms, prohibited overclaims; and which result
classes may appear in title, abstract, and conclusions.

## Independence, thresholds, and early feasibility

Define original units, independent clusters, effective independent units,
shared upstream data, independent data-generating processes, resampling units,
threshold classes, sensitivity analyses, and baseline decision-loss metrics.
Specify an outcome-blind real-record construct canary and a pre-analysis methods
pilot. Synthetic schema examples are not feasibility evidence.

## Novelty and recall plan

Define databases, query families, closest known work, citation searches,
counterevidence, and the stopping or saturation rule.

Assign every central claim an evidence role, minimum independent sources, and
minimum full texts. State how closest work is obtained and dispositioned before
novelty language is frozen.

## Deliverables

Define manuscript, figures, tables, supplement, data, code, reporting guideline,
and journal-specific submission materials.

## Stop, handoff, and pivot conditions

Define conditions for additional autonomous work, user authorization, external
response, research redesign, article-type change, or journal change.
