# Production research workflow

Load this reference for production initialization, a resumed/blocked project,
broker configuration, or recovery. The CLI remains authoritative when this
reference and runtime output differ.

## Production admission checklist

Before `project init`, record and show the user:

1. The research question and evidence dimensions.
2. Required source types, minimum sources, minimum full-text sources, and any
   date/applicability boundaries.
3. Available immutable inputs and locked capabilities that can satisfy each
   requirement, including the selected external internet profile and every
   owner-whitelisted database marked required for discovery.
4. Gaps that would block the post-discovery coverage gate.
5. Package token reservations, configured price basis, maximum cost, and
   whether the confirmation threshold is crossed.

Use `research project preflight`; do not calculate a competing checklist in the
skill. Production `project init` requires its evidence-requirements file and,
when the configured threshold is crossed, explicit `--confirm-budget`.

## Reproducible route configuration

Production mode requires different agent families, explicit model IDs, and
explicit pricing for producer and reviewer. `binary` must be the exact agent
name or an absolute executable path. Prefer the exact `codex` / `claude`
routes. If an explicit wrapper is required, set its absolute path in `binary`
and the underlying absolute agent path in `wrapperTargetBinary`; never use a
wrapper with an unpinned PATH lookup. The runtime separately records the target
binary, route launcher/wrapper, and internal adapter SHA-256 values, plus the
reported version, actual/configured model, OS, and architecture.
`workspace doctor --agent-smoke` executes both routes in real capsules and
writes a 24-hour attestation bound to those runtime fingerprints, workspace
config, capability lock, and doctor schema. Expiry or drift blocks execution
before the next agent invocation.

The budget includes:

- total tokens, cost, and wall time;
- a token reservation for every agent stage;
- maximum structured-output and repair tokens;
- output file count/bytes and attempts;
- broker response bytes, context items, and estimated context tokens;
- the cost-confirmation threshold.

New workspaces default to 500,000 total tokens, with 200,000 reserved as the
discovery package ceiling; analyze, synthesize, and review default to 55,000,
60,000, and 120,000. These values are admission ceilings, not a target spend.
Lower them only when preflight proves every complete reservation still fits.

The CLI will not start a package unless its full token and conservative price
reservation fits. Immediately before each call it accounts for prompt and
schema bytes at three bytes per token, agent-specific protocol overhead,
input repeated for every permitted API turn, the maximum bounded broker context
for every permitted discovery turn, primary output, and a possible isolated
repair's input and output. The provider cost cap is derived from that package
reservation, not the project's remaining global allowance. Tool-free
primary stages allow two protocol turns because Claude's schema output uses
`StructuredOutput` followed by its result; external tools remain disabled. The
repair path omits the provider schema tool, requests plain JSON in one turn,
and still passes through the same CLI validators.
Current Codex and Claude CLI adapters expose final output usage only after the
call. Preflight therefore reports `outputTokenLimitEnforcement` as
`post-execution`: captured bytes bound the process, and an over-limit result is
rejected without promotion, but the limit is not a provider-side hard stop.
Discovery capture allowance includes bounded broker tool-result events as well
as the requested final output.
Reserve enough output for the discover schema and enough input context for the
embedded prior-stage artifacts; preflight reports both gaps mechanically.
It also reports per-stage `maxTurns` and route-specific
`turnLimitEnforcement`. Claude receives its cap from the provider CLI. The
current Codex CLI has no turn-limit flag, so its value is a conservative
reservation assumption followed by actual-usage rejection; do not describe it
as a provider hard stop.

## Bounded local evidence

Use the same absolute `--input-plan` for preflight and project initialization.
Each entry registers the exact full source plus evidence dimensions, source
type, full-text claim, publication date, and either:

- `contextPath`: a separate regular, non-symlink excerpt whose own SHA-256 is
  admitted; or
- `contextRanges`: sorted, non-overlapping, one-based inclusive line ranges
  from a UTF-8 source.

```json
{
  "schemaVersion": 1,
  "inputs": [
    {
      "path": "/absolute/path/to/source.txt",
      "contextRanges": [{ "startLine": 20, "endLine": 80 }],
      "role": "primary",
      "dimensions": ["energy", "water"],
      "sourceType": "peer-reviewed",
      "fullText": true,
      "publicationDate": "2024-01-15"
    }
  ]
}
```

The producer receives only the derived bounded context. The full, hash-verified
source is withheld until independent review and is then listed in the review
packet. The CLI rejects symlinks, duplicate source content, overlapping or
out-of-range slices, changed hashes, and aggregate context above
`maxInputContextTokens`. Declaring `fullText: true` means the exact full file is
registered for review; it does not expose that entire file to discovery.

## Authoritative output contracts

Inspect, but never copy or fork, a current contract with:

```bash
npx --yes @tiangong-ai/cli@0.0.24 research schema show discover --json
npx --yes @tiangong-ai/cli@0.0.24 research schema show analyze --json
npx --yes @tiangong-ai/cli@0.0.24 research schema show review --json
```

Evidence sources include stable ID/title/locator/provenance, URL or DOI when
safe and available, source type, publication/retrieval dates, full-text status,
an excerpt or JSON Pointer, quality rationale, applicability, and covered
dimensions. The CLI verifies input locators and every broker object. Findings
may cite only admitted source IDs.

Codex receives `--output-schema`; Claude receives `--json-schema`. The CLI
validates the final object and materializes it atomically. Invalid syntax or
schema, or a mechanically diagnosed invalid provenance/finding binding, gets
one low-budget formatting-only repair with no broker access or research tools.
The repair may not add facts. A second failure stops the package instead of
repeating the research call.

## Broker evidence

A `brokered-network` capability has exact hosts plus an HTTP policy:

```json
{
  "id": "method.public-source",
  "skillPath": "/absolute/path/to/public-source-skill",
  "permissions": ["project-read", "candidate-write", "brokered-network"],
  "allowedHosts": ["api.example.org"],
  "http": {
    "method": "GET",
    "accept": "application/json",
    "allowedContentTypes": ["application/json"],
    "staticHeaders": {},
    "maxRequestBytes": 0,
    "maxResponseBytes": 524288,
    "maxItems": 100
  },
  "coverage": {
    "dimensions": ["research-question"],
    "sourceTypes": ["primary"],
    "fullText": true,
    "publicationDates": true
  },
  "credentials": []
}
```

Each successful body is written to the immutable content-addressed evidence
store before its capsule is deleted. A receipt binds the response hash/size,
content type, hashed final URL, raw locator, bounded context locator, retrieval
time, and cache status. Review packets enumerate and hash these permanent
objects.

The review capsule merges the registered local bounded contexts and every
cited broker receipt's exact bounded view. That merged context and the complete
review packet are themselves stored by content hash under the project's
`review/contexts/` and `review/packets/` directories. The packet enumerates raw
broker objects and full local-file hashes; the model reads only the merged
bounded views. Mechanical closure checks that the packet, merged context,
broker objects, and local input/context hashes still exist and match before it
records their safe locators.

Use `json_pointer` and `max_items` for a bounded JSON view. The workspace's
`maxBrokerContextTokens` limit additionally caps the staged view using a
conservative `ceil(contextBytes / 3)` estimate while retaining the exact raw
object. For a collection, continue within that object by sending the returned
`contextNextOffset` as `item_offset`; each view has its own receipt and context
object, while the raw response is reused without another upstream fetch.
Follow upstream pagination only through its next admitted HTTPS URL so each
upstream response receives its own permanent receipt. Public requests default
to the persistent cache; pass `cache_mode=bypass` for a fresh request.
Credentialed requests always require bypass to avoid replaying scoped content
under changed authorization.

The broker sends the capability's exact `Accept`, checks every redirect host,
screens credential-like response material, and rejects undeclared content
types or oversized bodies. Non-2xx results include only status, a sanitized
short response, safe request ID, and parsed `Retry-After`.

Capabilities may instead declare bounded JSON `POST`, an exact safe static
header map, and a positive `maxRequestBytes`. Discovery then supplies only the
documented non-secret `request_body`; credential-like keys are rejected, the
credential is injected by the broker, POST redirects are refused, and the
journal/cache metadata records only the canonical body SHA-256 rather than the
body. GET remains the default for existing imported definitions.

## Coverage, retry, and recovery

Discovery output is promoted for diagnosis, then checked mechanically. The CLI
derives local full-text availability, source types, source counts, dated counts,
date range, per-dimension source IDs, and the pass/insufficient decision from
admitted sources and declared minimums. `partial` is usable but incomplete;
`missing` or any unmet source/full-text/date minimum blocks all downstream
packages. Model-provided qualitative gaps remain visible but cannot override
those derived fields.

Discovery receives no shell or filesystem tools. The CLI embeds the locked
capability manifest and each staged external Skill's top-level `SKILL.md`, and
the scoped broker is its only execution tool. Broker results include the exact
bounded view inline with the receipt. Analyze embeds the admitted evidence
object; synthesize embeds admitted evidence and analysis. Both stages run with
tools disabled. Review is also tool-free and embeds the immutable packet,
generated artifacts, and exact bounded local/broker evidence views within two
structured-output protocol turns. Full files and raw
objects remain hash-bound for later human/mechanical audit; never report that
the model read beyond the embedded views. Run records, journal usage, and JSONL
progress include sanitized event/item counts, provider turns, tool calls,
reasoning-token counts, and bounded provider errors.

Failures are classified:

- configuration/authentication/deterministic 4xx, coverage, and review failures
  stop immediately;
- structured-output failures use only the repair path;
- 429 may retry after its recorded delay;
- bounded transient/5xx failures may retry while attempts and budget remain.

Use an explicit management command instead of editing state:

```bash
npx --yes @tiangong-ai/cli@0.0.24 research project retry PROJECT \
  --package PACKAGE --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.24 research project fork SOURCE \
  --to TARGET --resume-through analyze \
  --workspace /absolute/path/to/workspace --json
```

Retry grants one auditable extra attempt and resets downstream scheduling while
preserving promoted files. Fork starts a new budget/accounting history and may
inherit only verified completed discovery/analysis/synthesis artifacts; review
and closure always run again.

Run one recovered or canary project with an explicit scope:

```bash
npx --yes @tiangong-ai/cli@0.0.24 research run \
  --project PROJECT --workspace /absolute/path/to/workspace \
  --progress-jsonl --json
```

The top-level result and run-level JSONL events then carry that `projectId`, and
historical blocked siblings do not change its exit code. Omit `--project` only
when the operator intends to schedule and summarize the whole workspace.

## Standard zero-cost eval before a real canary

Confirm the owning CLI release passed deterministic mock coverage for:

- a clean empty directory, pinned external installer plan, and explicit
  installed/configured/locked/credential/live statuses;
- missing external Skills, missing credentials, unavailable provider plans,
  symlinked or drifting trees, and actionable structured failures;
- explicit owner-database import, required-discovery receipt enforcement, and
  local-only production rejection;
- routing and smoke/production mode boundaries;
- discover → analyze → synthesize → review → close;
- permanent evidence and review-packet hash verification;
- persistent bounded review-context verification and packet/context tamper
  rejection before closure;
- malformed JSON repair and a second-failure stop;
- invalid provenance/finding binding repair without repeating research;
- 429, deterministic 4xx/422, 5xx, redirect, oversized response, cache, and
  bounded offset extraction with raw-object reuse;
- capsule HOME/sandbox startup and evidence/journal recovery;
- bounded local producer context with full-source reviewer staging;
- broker-only discovery with embedded locked Skill documentation and tool-free
  analyze/synthesize/review stages;
- a broker result larger than the historical 64 KiB capture floor;
- capability drift and evidence tampering;
- insufficient evidence blocking closure;
- secret redaction across output, provider telemetry, errors, progress,
  journal, and manifests.
- project-scoped execution in a workspace containing historical blockers.

Only then run a bounded real-model canary with reservations that pass preflight.
Do not infer production readiness from a Crossref-only or single-source smoke
test.
