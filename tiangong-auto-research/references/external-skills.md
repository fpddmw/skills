# External evidence Skills

Use this reference when starting from a clean directory, selecting an internet
profile, installing or diagnosing recommended Skills, or admitting a database
the workspace owner is authorized to query.

## Contents

- [Boundary and source of truth](#boundary-and-source-of-truth)
- [Clean-directory setup](#clean-directory-setup)
- [Profiles and recommended Skills](#profiles-and-recommended-skills)
- [Credentials and live checks](#credentials-and-live-checks)
- [Status interpretation](#status-interpretation)
- [Owner-whitelisted databases](#owner-whitelisted-databases)
- [Evaluated alternatives](#evaluated-alternatives)
- [Failure rules](#failure-rules)

## Boundary and source of truth

Research method implementations are external to Tiangong Auto Research and the
Tiangong Skills repository. The CLI catalog records which external Skills were
reviewed, their immutable source commit and whole-tree hash, why each is or is
not recommended, and how the current workspace is configured.

Use the machine-readable catalog as the authority:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability catalog \
  --path /absolute/path/to/workspace --json
```

The runtime never installs dependencies. Installation happens as an explicit
owner action before capability configuration. A Skill only documents how to
query a source; the broker separately enforces exact HTTPS hosts, GET-only
request policy, response bounds, logical credentials, and permanent evidence
receipts.

The goal is broad, best-effort coverage across every source the owner has
selected and is authorized to use. It is not a claim that any provider indexes
the entire internet or that an unavailable subscription can be bypassed.

## Clean-directory setup

Prerequisites:

- Node.js 24, `git`, and `npx`;
- authenticated Codex and Claude CLIs for the producer/reviewer routes;
- macOS `/usr/bin/sandbox-exec` or Linux Bubblewrap (`bwrap`);
- a Brave Search API key and a plan that exposes Web and News Search for the
  baseline internet profile;
- any owner database credentials required by explicitly imported capabilities.

From the empty directory, verify the pinned CLI and inspect the plan before
creating research state:

```bash
cd /absolute/path/to/workspace
npx --yes @tiangong-ai/cli@0.0.23 --version
npx --yes @tiangong-ai/cli@0.0.23 research capability catalog \
  --path /absolute/path/to/workspace --json
```

For the baseline profile, the catalog's `installer.projectPlan.commands`
performs this exact sequence with absolute paths:

```bash
git init --quiet /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6
git -C /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6 remote add origin https://github.com/brave/brave-search-skills.git
git -C /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6 fetch --depth 1 origin 3e088af66eb61f1c207c22b2be0278ca8744d1d1
git -C /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6 checkout --detach FETCH_HEAD
test "$(git -C /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6 rev-parse HEAD)" = 3e088af66eb61f1c207c22b2be0278ca8744d1d1
npx --yes skills@1.5.22 add /absolute/path/to/workspace/.tiangong-external-skills/sources/brave-search-skills-3e088af66eb6 \
  --skill web-search news-search --agent codex --yes --copy
```

Do not copy this example with a different workspace path by guesswork. Prefer
the absolute commands returned for the current directory, review them, and run
them one at a time. Do not pipe catalog output into a shell. The checkout
directory must not already exist; an existing directory is a state to inspect,
not permission to overwrite it.

For enhanced or media profiles, use each selected entry's
`install.projectPlan`, or the catalog's `allRecommendedProjectPlan` to install
all five reviewed recommendations. The CLI verifies every selected installed
tree before writing a lock, so merely seeing a directory is not success.

Then initialize and configure the project-local copy:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research workspace init \
  /absolute/path/to/workspace --mode production-research --json
npx --yes @tiangong-ai/cli@0.0.23 research capability configure \
  --profile internet-research \
  --skill-root /absolute/path/to/workspace/.agents/skills \
  --workspace /absolute/path/to/workspace --json
```

Never treat an unpinned globally installed copy as a substitute for the
project-local plan when reproducibility matters.

## Profiles and recommended Skills

Choose one profile explicitly:

| Profile | Selected Skills | Use |
| --- | --- | --- |
| `internet-research` | `web-search`, `news-search` | Default public-internet discovery; both are required. |
| `internet-research-with-context` | baseline plus `llm-context` | Use only when the provider plan exposes LLM Context and extracted page context materially improves the study. |
| `internet-research-with-media` | context profile plus `images-search`, `videos-search` | Use when visual or audiovisual sources are material evidence. |

All current recommendations come from external repository
`brave/brave-search-skills` at commit
`3e088af66eb61f1c207c22b2be0278ca8744d1d1` under MIT:

| Skill | Tier | Expected whole-tree SHA-256 |
| --- | --- | --- |
| `web-search` | required | `0432f4eb084766046a2feeb146f0b3917850d138eb4182c23fc000a058fbe123` |
| `news-search` | required | `80f8dcb7c78209cce5315e507f0aba21e3f654f42f6a414c177de644bcd07773` |
| `llm-context` | enhanced, plan-dependent | `5abba551d0498a80eba4e64207f483974f5a312cda5b263c1cc2b7f99d81c4a3` |
| `images-search` | conditional | `467a9afae0e959b482cb6b2236a57a327959b28985871f88c202dc70b10a7c84` |
| `videos-search` | conditional | `ff3a2e2291efc27f3f09847b7e0b20e77d229f6a44a1a64e562964c820eb9719` |

Do not silently fall back from a profile when one selected endpoint is missing
or unavailable. Report the endpoint/plan failure and ask the user to either fix
access or explicitly select a different profile.

## Credentials and live checks

The five Brave Skills share logical credential ID `brave.search.api-key`.
Obtain the owner key through the provider's normal account flow and expose it
to the configuration process as `BRAVE_SEARCH_API_KEY`. Do not place the value
in a command argument, document, capability definition, or chat message.

Store it through the non-echoing logical-credential command:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability credential set \
  --id brave.search.api-key --from-env BRAVE_SEARCH_API_KEY \
  --workspace /absolute/path/to/workspace --json
```

The command writes only the owner-only workspace credential store and never
returns the value. Agent processes do not receive it; the broker injects it for
the declared host only.

Run both static and live verification before production preflight:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability verify \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability doctor --live \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research workspace doctor \
  --workspace /absolute/path/to/workspace \
  --agent-smoke --capability-smoke --json
```

`llm-context`, image, and video access may differ by provider plan. A key that
passes Web Search does not prove every selected endpoint is enabled.

## Status interpretation

Call catalog again with the workspace to obtain one complete status view:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability catalog \
  --path /absolute/path/to/workspace \
  --workspace /absolute/path/to/workspace \
  --skill-root /absolute/path/to/workspace/.agents/skills --json
```

Interpret the machine fields rather than directory presence:

- `installation.status=missing`: run the returned pinned plan.
- `installation.status=drifted`: the discovered tree does not match the
  catalog hash; restore the exact source instead of re-locking it.
- `installation.status=ambiguous`: more than one exact candidate is visible;
  select one explicit project root.
- `installation.status=installed`: exactly one safe tree matches.
- `status.configured=false`: the Skill is installed but not declared in this
  workspace.
- `status.locked=false`: configuration and verified content lock disagree.
- `status.credential=not-configured`: the Skill is not selected in this
  workspace; `missing` means it is selected but its logical value is absent;
  `configured` means the value is present; `not-required` means the capability
  declares no credential; `blocked` means credential inspection itself failed.
  A `missing` or `blocked` state authorizes no provider request.
- a live doctor failure means package execution must not start. Authentication,
  subscription, deterministic 4xx, unsafe content type, and drift failures stop
  immediately; one bounded 429 retry is the only automatic live-check retry.

Catalog output also includes actionable installation commands, provider-plan
text, exact hosts, credential IDs, summary counts, and evaluated alternatives.
Show these statuses to the user before spending model budget.

## Owner-whitelisted databases

An owner-authorized database is another external capability; it is not ambient
agent access. Start from `customExternalCapabilities.brokeredEvidenceDefinitionTemplate`
in catalog output and create an external JSON definition containing:

- an absolute path to the separately installed external Skill;
- a top-level `SKILL.md` containing the complete bounded GET endpoint and
  response-shape instructions needed for discovery. The runtime embeds that
  file and the capability manifest; discovery has no filesystem tool with which
  to follow additional reference links;
- external source identity, full immutable git commit/exact registry version or
  local content reference, license, and expected whole-tree SHA-256;
- exact HTTPS hosts and a bounded GET response policy;
- a safe health check;
- coverage dimensions, source types, discovery scopes, full-text/date claims;
- logical credential IDs and exact host/header scopes, never credential values;
- `requiredForDiscovery: true` when the study must query that database.

Import, configure the declared key, and probe the real endpoint:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability import \
  --definition /absolute/path/to/external-capability.json \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability credential set \
  --id database.owner-source.api-key --from-env OWNER_DATABASE_API_KEY \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability doctor --live \
  --workspace /absolute/path/to/workspace --json
```

Do not import a Tiangong project-owned Skill as an evidence provider, import a
symlinked tree, broaden hosts with wildcards, authorize POST through the current
GET broker, or convert a login/session cookie into a reusable database key.

## Evaluated alternatives

The catalog records all other Skills reviewed from the pinned Brave source:

- `answers`: not admitted because it synthesizes through POST and is not raw
  evidence under the GET broker.
- `bx`: not admitted because it delegates to another executable and credential
  store outside the broker boundary.
- `local-descriptions` and `local-pois`: install and explicitly import only for
  a research question with a material place/POI dimension.
- `spellcheck` and `suggest`: query helpers, not independently reviewable
  evidence sources.

These dispositions are explicit review results, not hidden automatic choices.

## Failure rules

- Missing package, Skill, key, provider plan, or binary: return the structured
  catalog/doctor error and its minimum user action; do not install at runtime.
- Tree/source drift or ambiguity: stop before lock or execution.
- Authentication, authorization, subscription, or database access failure:
  stop and report; never bypass it.
- Local-only production plan: stop. A locked external public-internet
  capability is mandatory, and every capability marked `requiredForDiscovery`
  must produce its own receipt.
- Secret-like URL parameters, Authorization/Cookie headers, API keys, and
  tokens must not appear in output, events, journal, evidence, or manifests.
- Successful live checks prove endpoint connectivity only. The post-discovery
  coverage gate still decides whether the collected evidence is sufficient.
