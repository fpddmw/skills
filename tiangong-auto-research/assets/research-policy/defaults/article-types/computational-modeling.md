---
schemaVersion: 1
id: article.computational-modeling
kind: article-type
templateClass: bundled-default
policyVersion: 1
targetTier: top
articleType: computational-modeling
rules:
  - model-calibrated-or-justified
  - independent-validation-required
  - uncertainty-propagated
  - baseline-comparison-required
  - material-results-reproduced
constraints:
  minDirectPeerReviewedFullText: 5
  minDirectModelFullText: 1
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

# Default computational and modeling research policy

## Scope

Use when the central contribution is a computational method, simulation,
integrated model, forecast, or quantitatively tested mechanism.

## Editorial significance

Require a capability, mechanism, or decision insight unavailable from existing
models, not merely a new scenario using familiar equations.

## Evidence expectations

Bind each parameter and calibration target to applicable evidence. Distinguish
assumed, fitted, transferred, and independently measured values.

## Methods and validation

Require baseline comparisons, identifiability or sensitivity analysis,
uncertainty propagation, failure cases, and independent empirical validation
where the paper makes real-world claims.
Classify cross-model comparison, mechanism-model output, scenario output,
accounting output, calibrated estimates, and independently observed validation
as different result classes. Declare shared upstream data, independent
data-generating processes, original units, independent clusters, effective
units, and the cluster-preserving resampling unit. More bootstrap iterations do
not create more independent observations.

## Reproducibility

Freeze executable code, environment, seeds, inputs, configuration, and the
complete table and figure generation path.

## Desk-reject triggers

- The main result follows algebraically from assumptions.
- Calibration and validation use the same evidence without justification.
- A scenario analysis is presented as a forecast or observed effect.
- A model discrepancy is presented as field error without a declared observed
  truth endpoint, or gross installed material is relabeled as a narrower or
  network-wide quantity without a supported conversion.
- Code or material parameters cannot be independently reconstructed.

## Required reviewer questions

- What observation could falsify the modeled mechanism?
- Does the model outperform a credible simple baseline?
- Are uncertainty and parameter dependence visible in the main conclusions?
- Is validation independent of model construction?

## Permitted pivots

Narrow the claim to an illustrative framework, obtain validation data, publish
the method or resource separately, or convert the work to a Perspective.
