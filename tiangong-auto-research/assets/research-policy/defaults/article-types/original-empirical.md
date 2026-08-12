---
schemaVersion: 1
id: article.original-empirical
kind: article-type
templateClass: bundled-default
policyVersion: 1
targetTier: top
articleType: original-empirical
rules:
  - central-outcome-observed
  - central-claim-directly-supported
  - alternative-explanations-tested
  - robustness-evidence-required
  - material-results-reproduced
requiredReviewers:
  - evidence
  - methods-reproducibility
  - domain-novelty
  - journal-editor
reviewAfterDays: 180
---

# Default original empirical research policy

## Scope

Use for papers whose central contribution is a new observation, estimate,
experiment, causal result, or empirically validated prediction.

## Editorial significance

Require a result that changes understanding, practice, or a material decision;
sample size or a new setting alone is not sufficient novelty.

## Evidence expectations

Require direct data for the central outcome, a justified sampling frame, and
evidence that supports the claimed population, place, and period.

## Methods and validation

Require an identified estimand or target quantity, a defensible design,
diagnostics, uncertainty, robustness checks, and explicit tests of plausible
alternative explanations. Prediction claims require external or genuinely
held-out validation.

## Reproducibility

Recompute every material number, table, and figure from exact registered data
and code in a clean environment. Unreproduced owner results are reference-only.

## Desk-reject triggers

- The central outcome is not observed or validated.
- Main results are illustrative algebra or hard-coded statistics.
- The title generalizes beyond the study design or evidence.
- The closest prior empirical studies are absent from the novelty comparison.

## Required reviewer questions

- Is the central estimate identified by the design rather than asserted?
- Which robustness result would most likely reverse the conclusion?
- Does the evidence support the claimed external scope?
- Is the empirical contribution clearly distinct from the closest prior work?

## Permitted pivots

Collect or obtain additional data, narrow the estimand and scope, convert the
work to a methods or modeling paper, or recast it as a Perspective.
