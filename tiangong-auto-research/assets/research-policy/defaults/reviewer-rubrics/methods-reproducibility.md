---
schemaVersion: 1
id: reviewer.methods-reproducibility
kind: reviewer-rubric
templateClass: bundled-default
policyVersion: 1
targetTier: top
reviewerRole: methods-reproducibility
rules:
  - design-supports-central-claim
  - material-results-reproduced
  - robustness-and-uncertainty-reviewed
reviewAfterDays: 180
---

# Default methods and reproducibility reviewer rubric

## Scope

Audit the design, data, code, parameters, computation, uncertainty, validation,
and robustness for the central result.

## Editorial significance

Determine whether the methodological advance or empirical design is strong
enough to support a top-journal contribution rather than a working example.

## Evidence expectations

Require every material parameter and validation target to have an admissible,
applicable source or an explicit tested assumption.

## Methods and validation

Challenge identification, controls, leakage, baselines, sensitivity, external
validity, uncertainty, failure cases, and alternative explanations.
Recompute original units, independent clusters, effective independent units,
and resampling units. Verify endpoint truth roles, shared upstream data,
quantity/denominator scope, threshold classes, and decision-loss metrics. A
large resampling count cannot override four independent structures, a circular
validation route, or an unobserved endpoint.

## Reproducibility

Run or inspect the clean reproducibility record from exact data and code through
every material table and figure.

## Desk-reject triggers

- A central statistic or figure cannot be recomputed.
- Results are identities or scenarios presented as observed effects.
- Validation is absent, circular, or outside the claimed population.
- A real-record construct canary is missing, inspected outcomes before freezing
  the method, or failed a central evidence edge.

## Required reviewer questions

- Can an independent group reproduce the main result?
- Which assumption most threatens the conclusion?
- What baseline or falsification test is missing?

## Permitted pivots

Require redesign, more data, narrower claims, or a methods/Perspective route.
