# Recommended external Skills

Use this reference to explain what Auto Research recommends, what each Skill is
allowed to do, which credentials it needs, and how installation is governed.
The live machine-readable source of truth is:

```bash
npx --yes @tiangong-ai/cli@0.0.26 research setup catalog \
  --workspace /absolute/path/to/workspace --json
```

Every entry is external to the Auto Research runtime. It is separately sourced,
never bundled, never installed at runtime, and selected only by the user. The
catalog binds an immutable git commit and whole-tree SHA-256 for every Skill.

## Why setup manages `npx skills`

The underlying cross-agent installer is `skills@1.5.22`, invoked as
`npx --yes skills@1.5.22 add ... --copy`. Users can use `npx skills` directly for
general Skill management, but Auto Research should use `research setup` because
it additionally freezes and verifies:

- npm integrity/shasum/git head for the installer;
- exact source repository and commit;
- selected subdirectory and expected tree hash;
- project/global destination and target agent;
- license acceptance, credential names, settings, checks, and mutations;
- installed-byte status and an append-only audit trail.

Setup does not trust a directory merely because it exists and never silently
adopts or overwrites a drifted copy. Project-local `.agents/skills` or
`.claude/skills` copy mode is the reproducible default. Global scope is an
explicit operator choice.

## Evidence capabilities

These Skills can contribute candidate evidence only through the locked HTTP
broker during discovery:

| Catalog IDs | Source | Role and selection rule | Credential |
| --- | --- | --- | --- |
| `brave.web-search`, `brave.news-search` | `brave/brave-search-skills` | Baseline public-internet profile; both required for production. | `brave.search.api-key` from an owner variable such as `BRAVE_API_KEY` |
| `brave.llm-context` | same | Enhanced bounded page context when the provider plan supports it. | same Brave key |
| `brave.images-search`, `brave.videos-search` | same | Conditional visual/audiovisual discovery only when material to the question. | same Brave key |
| `tiangong.kb-sci-search` | `tiangong-ai/skills` | Optional owner-whitelisted academic database. It supplements, but never satisfies, the production public-internet gate. | `tiangong.sci.api-key` from `TIANGONG_SCI_APIKEY` or another explicitly named owner variable |

Brave capabilities use their manifest-declared GET endpoints. Tiangong SCI uses
the exact declared JSON POST endpoint, `x-region`, and `x-api-key` credential
injection. The discovery agent supplies only non-secret `request_body` fields;
the broker rejects credential-like fields, persists only the request-body hash,
refuses POST redirects, bounds the response, and promotes the exact response to
the permanent evidence store.

Do not run a staged Skill's shell/curl examples inside a research capsule. Agent
processes never receive provider credentials. Authentication, authorization,
subscription, deterministic 4xx, unsafe response type, and source drift stop
execution; no source or login barrier may be bypassed.

## Input preprocessing and acquisition

These are installed companions, not `PaperTransport` implementations and not
broker evidence capabilities:

| Catalog ID | Role | Configuration | Boundary |
| --- | --- | --- | --- |
| `tiangong.document-granular-decompose` | `input-preprocessor` | Required `tiangong.unstructure.auth-token`; required HTTPS base URL; optional provider/model. | Explicit upload outside research execution. The CLI hash-binds input/output and atomically commits a new output. Admit that output separately. |
| `tiangong.academic-paper-download` | `acquisition-adapter` | Optional Semantic Scholar API key and optional Unpaywall contact email; Python 3.10+ and `pypdf==6.14.2`. | Automatic legal OA order stays Unpaywall → Semantic Scholar OA → arXiv → explicit browser handoff. It preserves its own PDF/size/hash/manifest finalization. |

The paper adapter never substitutes CloakBrowser for Chrome, never silently
switches browser backends, and never uses a browser to replace Crossref,
Unpaywall, Semantic Scholar, or arXiv requests. Browser handoff remains a
separate user-authorized workflow in that Skill. CloakBrowser is an optional,
separately pinned dependency with its own persistent profile; it is not installed
by research setup and must not import Chrome profiles, cookies, passwords,
tokens, or API keys.

## Post-closure authoring

The following may be selected only for optional artifact work after mechanical
research closure. They cannot act as evidence providers or mutate admitted
research artifacts:

| Catalog IDs | Source | License/configuration note |
| --- | --- | --- |
| `anthropic.doc-coauthoring` | `anthropics/skills` | No API key. The pinned Skill tree has no per-Skill license file; catalog reports `NOASSERTION`, so the user must review and explicitly accept the notice. |
| `anthropic.docx`, `anthropic.pdf`, `anthropic.pptx`, `anthropic.xlsx` | `anthropics/skills` | No API key. Source-available Anthropic document terms are not open source; nothing is bundled and installation is opt-in. Use `anthropic.pptx` when its reading, editing, or alternative authoring workflow better fits the task. |
| `hugohe3.ppt-master` | `hugohe3/ppt-master` | Preferred for creating PPT presentations. MIT, no API key. Upstream Python requirements contain ranges; setup refuses to resolve/install them until the user creates and reviews an exact isolated lock. |

`anthropic.pptx` and `hugohe3.ppt-master` are compatible selections and may be
installed by the same explicit setup plan. This is a task preference, not an
installation conflict: default to PPT Master when creating a PPT, then use
Anthropic PPTX or another selected authoring Skill when the concrete task makes
that workflow a better fit. Setup does not auto-select either Skill.

## Credential and setting matrix

| Logical configuration | Required when | How to obtain/configure |
| --- | --- | --- |
| `brave.search.api-key` | Any Brave profile | Create a key through the normal Brave Search API account flow; give setup only the environment variable name. |
| `tiangong.sci.api-key` | Tiangong SCI selected | Ask the owner of the authorized deployment; do not reuse browser/session credentials. |
| `tiangong.sci.endpoint`, `tiangong.sci.region` | Tiangong SCI selected | Review the exact HTTPS endpoint and non-secret region shown by the Wizard. |
| `tiangong.unstructure.auth-token` | Document preprocessing selected | Ask the deployment owner; stored only in the adapter credential store. |
| `tiangong.unstructure.base-url` | Document preprocessing selected | Exact HTTPS service base URL; provider/model overrides are optional non-secret settings. |
| `semantic-scholar.api-key` | Optional | Obtain from Semantic Scholar's normal API application flow; anonymous access remains supported by the paper Skill. |
| `unpaywall.contact-email` | Optional | A real contact email, stored as a non-secret setting. |

No key is accepted as a command argument, JSON request field, manifest value, or
chat value. Setup's `credential set` command reads a named owner environment
variable and returns only the logical ID and storage class.

## Status and live checks

Interpret `research setup status` and `research setup doctor`, not directory
presence:

- `missing`: the reviewed tree is absent;
- `installed`: exact bytes match;
- `drifted`: bytes differ; do not re-lock or overwrite them;
- `blocked`: path, symlink, permissions, dependency, credential, or check
  policy failed;
- `READY`: every selected required static/live check passed;
- `PARTIALLY_READY`: deferred optional/cost/network checks remain;
- `BLOCKED`: a selected required capability or dependency is unusable.

Live checks are explicit because they use network/quota. A key that passes one
Brave endpoint does not prove that LLM Context, image, or video access is
included in the provider plan. The Unstructure live check uploads a generated
one-page PDF only with its separate confirmation. Agent smoke is separately
cost-confirmed.

## Custom owner-whitelisted databases

A source not in the recommendation catalog must be imported explicitly as an
external capability definition. Bind its absolute non-symlink Skill tree,
immutable source identity/hash/license, exact HTTPS hosts, GET or bounded JSON
POST policy, safe health check, coverage claims, logical credential scope, and
`requiredForDiscovery` decision. Never place a credential value in the
definition.

An owner database can broaden authorized coverage but cannot be described as
the entire internet and cannot replace the independent public-internet gate.
If access, subscription, VPN, SSO, MFA, or entitlement is unavailable, stop and
report the minimum legitimate user action.
