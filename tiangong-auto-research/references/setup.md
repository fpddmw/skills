# External Skill setup

Use this reference to configure Tiangong Auto Research in a clean directory or
to inspect, resume, update, or replace an existing setup generation.

## Safety model

Setup is a separate owner operation, not part of a research package. No
recommended Skill is bundled with the CLI or installed implicitly. The CLI:

1. shows the complete recommendation catalog without writing files;
2. records exact source commits, Skill tree hashes, installer version and npm
   integrity, destinations, settings, credential environment names, licenses,
   checks, and mutations in an immutable plan;
3. checks required credentials before it downloads an installer or source;
4. copies only the selected pinned trees, verifies the installed bytes, and
   configures their declared execution role;
5. stores broker and adapter credentials in separate owner-only stores without
   printing values;
6. writes a sanitized doctor report and append-only journal events.

Project-local copy mode is the default. Global installation needs a separate
confirmation and exact targets are frozen into the plan. Setup never installs
system packages or Python packages, never runs `pip install`, never follows a
Skill destination symlink, and never performs a floating update.

## Clean-directory Wizard

Prerequisites are Node.js 24, `git`, `npx`, an agent-compatible sandbox
(`/usr/bin/sandbox-exec` on macOS or `bwrap` on Linux), and normal Codex/Claude
CLI authentication for the routes you intend to use. Create or choose the
directory, export only the credentials for sources you may select, and start
the Wizard:

```bash
mkdir -p /absolute/path/to/research-workspace
cd /absolute/path/to/research-workspace

# Required only when the corresponding source is selected.
export BRAVE_API_KEY='owner Brave Search key'
export TIANGONG_SCI_APIKEY='owner-authorized Tiangong SCI key'
export UNSTRUCTURED_AUTH_TOKEN='owner Unstructure bearer token'

# Optional; academic-paper-download can use Semantic Scholar anonymously.
export SEMANTIC_SCHOLAR_API_KEY='optional Semantic Scholar key'

npx --yes @tiangong-ai/cli@0.0.25 --version
npx --yes @tiangong-ai/cli@0.0.25 research setup \
  --workspace /absolute/path/to/research-workspace --json
```

Do not paste a secret when the Wizard asks for an environment variable name.
It displays only whether the named variable is present. It then guides the
user through:

- smoke-test versus production mode;
- public-internet profile;
- optional Tiangong companions and post-closure authoring Skills; for PPT
  creation it presents PPT Master as preferred while keeping Anthropic PPTX as
  a compatible situational choice;
- Codex/Claude install targets and project/global scope;
- non-secret endpoints/settings;
- credential environment names;
- each pinned source/license notice and explicit acceptance;
- optional model IDs and reviewed token prices;
- live provider checks, a separately authorized synthetic Unstructure upload,
  and separately authorized paid agent smoke checks;
- a full plan preview, network confirmation, immutable plan creation, and
  optional apply.

If a required key is absent, the plan may be saved but apply stops before any
network download. Set the named variable and resume the exact plan; do not
recreate the plan merely to bypass preflight.

## Read-only inspection and automation

The catalog command never creates workspace files:

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup catalog \
  --workspace /absolute/path/to/research-workspace \
  --scope project --agents codex --json
```

For non-interactive automation, first write JSON files containing only
non-secret mappings/settings. For example, `credential-env.json` contains
environment variable names, not values:

```json
{
  "brave.search.api-key": "BRAVE_API_KEY"
}
```

Create and review a minimal production plan, then apply its exact path:

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup plan \
  --workspace /absolute/path/to/research-workspace \
  --mode production-research \
  --evidence-profile brave-context \
  --agents codex --scope project \
  --credential-env /absolute/path/to/credential-env.json \
  --accept-license brave-search-skills:MIT \
  --confirm-network-downloads --json

npx --yes @tiangong-ai/cli@0.0.25 research setup apply \
  --plan /absolute/path/to/research-workspace/.tiangong-research/setup-plan.json \
  --json
```

Add `--skills` IDs, settings, credential mappings, and matching license IDs only
after reviewing `setup catalog`. Do not manufacture pins or hashes from this
document; the CLI catalog is authoritative.

## Status, doctor, and recovery

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup status \
  --workspace /absolute/path/to/research-workspace --json
npx --yes @tiangong-ai/cli@0.0.25 research setup doctor \
  --workspace /absolute/path/to/research-workspace --json
```

Static doctor checks installed tree hashes, required settings, owner-only
credential stores, Node/git/npx, sandbox, agent CLIs, Python, and pinned Python
requirements. `--live` uses provider network/quota. A synthetic document upload
also requires `--allow-synthetic-unstructure-upload`; model-agent smoke requires
`--agent-smoke --confirm-agent-smoke-cost`.

On a blocked apply, use the exact `retryCommand` in setup state. Clear a stale
lock only with both stale-lock flags after confirming no setup process owns it.
Do not delete plan/state/source-cache directories or overwrite installed trees.

## Update without version drift

`update --check` is read-only. The currently installed generation stays pinned:

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup update --check \
  --workspace /absolute/path/to/research-workspace --json
```

An upgrade is a new immutable plan, never an in-place floating update. Review
new licenses and pins, then apply the newly generated plan:

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup upgrade \
  --plan --confirm-upgrade \
  --accept-license <every-selected-current-license-id> \
  --workspace /absolute/path/to/research-workspace --json
```

The previous plan/state/report/config generation is archived under
`.tiangong-research/setup-history/<plan-sha256>/` for audit and recovery.

## Run selected companion adapters

Companions execute outside immutable research packages with a minimal child
environment and verified Skill tree. Document output uses a unique temporary
file and a no-overwrite atomic commit:

```bash
npx --yes @tiangong-ai/cli@0.0.25 research setup companion run \
  --id tiangong.document-granular-decompose \
  --input /absolute/path/to/source.pdf \
  --output /absolute/path/to/source.fulltext.md \
  --workspace /absolute/path/to/research-workspace --json
```

Paper acquisition binds only the precise adapter result and manifest; it never
chooses the newest PDF in a directory:

```bash
mkdir -p /absolute/path/to/papers
npx --yes @tiangong-ai/cli@0.0.25 research setup companion run \
  --id tiangong.academic-paper-download \
  --doi 10.1234/example --out /absolute/path/to/papers \
  --workspace /absolute/path/to/research-workspace --json
```

The adapter must complete its PDF header/EOF/pypdf/size/SHA-256/atomic manifest
pipeline before the CLI reports success. If automatic OA sources are exhausted,
the result is `browser-handoff-required` with no committed artifact. Follow the
installed Skill's browser-handoff reference manually; setup never launches a
browser or turns a browser into an evidence capability.
