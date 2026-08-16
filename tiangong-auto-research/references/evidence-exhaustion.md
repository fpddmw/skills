# Evidence exhaustion and access handoff

Use this protocol only when a required evidence role remains materially
unsatisfied after broad discovery and strict admission. It separates work the
current native host can still perform from work that requires a purchase or
subscription, user authorization, or an external response.

## Plan acquisition routes before search

The frozen scientific design maps each required evidence role to explicit
lawful routes. An agent route selects one auditable mechanism: a locked broker
capability, a named native activity channel, an OA download backend, or an
authorized browser backend. A non-agent route identifies licensed material,
owner-provided material, an external data request, or field collection.
Every declared agent route that maps a required evidence role must itself be
required. Every `requiredCapabilityId` must map to a required broker route whose
capability exists in the current verified lock; preflight rejects optional,
unmapped, or unavailable routes.

Bind every broker request with the exact design route in
`acquisition_route_id`. Bind native activity and download events with the exact
route in `acquisitionRouteId`. A missing, mismatched, or after-the-fact route ID
is rejected and cannot prove exhaustion.

Do not add a route after results are known merely to justify stopping. A
material route change requires a new authoritative scientific-design
generation and fresh applicable review.

## Distinguish immediate challenges from true exhaustion

An `interactive-challenge` handoff pauses immediately for login, SSO/MFA,
CAPTCHA, Turnstile, a security warning, VPN, entitlement, or a visible paywall.
It does not claim that other evidence routes are exhausted and it must never
trigger automatic bypass.

An `evidence-exhausted` handoff is valid only when all required,
agent-executable routes mapped to every cited missing required evidence role
have a verified terminal event hash. A completed search with insufficient
admissible evidence, an explicit broker authentication/entitlement block, or a
validated deterministic no-OA/unavailable download result may be terminal. A
malformed request, configuration mismatch, HTTP 422, timeout, 429, 5xx,
cancelled download, blocked interactive challenge, or unrecorded assertion is
not exhaustion.

Inspect the exact status before requesting this handoff:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project access status PROJECT \
  --workspace /absolute/path/to/workspace --json
```

Use only the returned route IDs, terminal event hashes, classifications, and
recommended action. If it lists an untried required agent route, continue that
route or revise the reviewed design; do not continue substitute searching
outside the plan. When all agent routes are terminal, first assess the required
evidence-role coverage. Follow `ifEvidenceStillInsufficient` only when a cited
required role genuinely remains below its reviewed floor.

## Make every access request actionable

For each remaining non-agent route, record:

- the exact required evidence role and material gap;
- the resource type and non-sensitive resource name;
- an official HTTPS locator for a database subscription, article purchase,
  institutional-access page, or licensed dataset when one exists;
- the lawful access mode and whether cost is unknown or requires a provider
  quote;
- the alternatives tried, identified by reviewed route IDs and terminal event
  hashes;
- the minimum user or external action;
- exact resume criteria, such as an authorized browser session, a registered
  owner input, or a hash-bound downloaded artifact.

Do not fabricate a price, subscription entitlement, database coverage, license,
response, full text, or evidence value. Never place credentials, cookies,
session data, authorization headers, sensitive URL parameters, or profile
paths in the record. The CLI sanitization and URL checks remain authoritative.

## Stop and resume

Use `user-action-required` for purchase, subscription, institutional login,
authorization, VPN, or owner-supplied material. Use
`external-response-required` when a government, institution, data owner, or
collaborator must respond. Once either state is durable, stop model work and do
not continue substitute searching.

If every required agent route is proven terminal and no lawful user or
external-party route remains, request the reviewed scope-pivot disposition with
empty remaining routes and access requests. The user must narrow or abandon
the unsupported scope or claim; absence of obtainable evidence is never a
positive result.

Resolve only after the requested condition is true. Resolution reopens the
same reviewed route; it does not mark the missing evidence role satisfied.
Evidence must still enter through the ordinary broker, input, download,
artifact, snapshot, scientific-review, and publication gates. If the resource
cannot be obtained, create a new reviewed scope or article-generation decision
instead of implying that unavailable evidence was observed.
