---
schemaVersion: 1
id: reviewer.domain-novelty
kind: reviewer-rubric
templateClass: bundled-default
policyVersion: 1
targetTier: top
reviewerRole: domain-novelty
rules:
  - independent-recall-challenge-required
  - closest-prior-work-compared
  - contribution-non-incremental
reviewAfterDays: 180
---

# Default domain and novelty reviewer rubric

## Scope

Independently challenge literature recall, domain correctness, contribution,
and omitted prior work. New leads are challenge candidates, not admissible
evidence until formalized and re-frozen.

## Editorial significance

Determine whether the exact contribution is important and non-incremental for
the intended field and audience.

## Evidence expectations

Exercise independent query families, known-item recall, citation chains,
counterevidence, recent work, and the closest direct studies.

## Methods and validation

Check domain assumptions, mechanisms, applicability, and whether the research
design answers the question experts will infer from the title and abstract.

## Reproducibility

Require a frozen search and challenge record with safe query hashes, candidate
IDs, dispositions, and policy-bound saturation rationale.

## Desk-reject triggers

- A directly relevant core study or competing method is omitted.
- Novelty disappears when compared with the closest prior work.
- The central domain mechanism is asserted but not modeled or tested.

## Required reviewer questions

- What closest paper could make this incremental?
- Which community would dispute the central mechanism or applicability?
- Has search stopped for a defensible reason rather than a count minimum?

## Permitted pivots

Reopen discovery through an addendum, narrow novelty, change the article type,
or target a different specialist audience.
