# Research environment

## Runtime prerequisites

- Node.js 24.
- `npx` access to `@tiangong-ai/cli@0.0.21`.
- Authenticated `codex` and `claude` executables, unless absolute binary paths
  are selected in `.tiangong-research/config.json`.
- macOS with `/usr/bin/sandbox-exec`, or Linux with Bubblewrap available as
  `bwrap`.

Authenticate the agent CLIs through their standard local login before running a
project. Capability service credentials use the workspace contract below and
are never agent environment variables.

If doctor reports that Claude is not logged in, start Claude interactively
outside the research workspace, complete its normal `/login` flow, then rerun
doctor. Do not copy tokens from keychains, browser profiles, or another agent
HOME into the workspace. If a custom agent wrapper is selected, use an absolute
path, keep the wrapper immutable for the run, and let doctor record and verify
its hash; do not add undocumented transport or authentication flags.

The tested macOS/Linux template is
`scripts/agent-wrapper-posix.sh`; validate it with
`scripts/test-agent-wrapper-posix.sh`. Configure the route explicitly:

```json
{
  "agent": "codex",
  "binary": "/absolute/path/to/agent-wrapper-posix.sh",
  "wrapperTargetBinary": "/absolute/path/to/codex",
  "model": "explicit-model-id",
  "effort": "low",
  "verbosity": "low"
}
```

The runtime sets `TIANGONG_RESEARCH_AGENT_BINARY` to the resolved target; do not
set or forward that variable yourself. The target, wrapper, and internal
adapter hashes are attested independently, and target drift stops execution.

The CLI creates a dedicated HOME for every capsule. It copies only the
supported minimal agent credential file when one exists (for example Codex
`auth.json`), never a complete Chrome/agent profile. For Claude, an owner-only
user `settings.json` is not copied: the runtime extracts only
`ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`, and a
credential-free HTTPS `ANTHROPIC_BASE_URL` from its `env` object. Permissions,
hooks, extra directories, and other settings are excluded. On macOS,
credentials held by the system keychain remain in the keychain. Production
doctor must be run with `--agent-smoke` to detect a missing login, first-launch
gate, unwritable state database, or sandbox incompatibility before an expensive
package starts.
The successful attestation lasts 24 hours and must be regenerated after config,
capability, schema, binary, or wrapper drift. Sanitized provider transport
diagnostics may appear in doctor details even when the provider subsequently
falls back and succeeds; use the final check status as the readiness decision.

## Capability credentials

The only accepted key in `.tiangong-research/.env` is:

```bash
TIANGONG_RESEARCH_CAPABILITY_CREDENTIALS_JSON={"source.example.api":"owner-provided-value"}
```

Requirements:

- The value is one JSON object on one line.
- Every key is a logical credential ID declared in
  `.tiangong-research/capabilities.json`.
- Every value is a non-trivial string of at least eight UTF-8 bytes.
- No shell interpolation syntax is supported.
- Duplicate assignments and undeclared IDs are rejected.
- On POSIX systems, set owner-only permissions:

  ```bash
  chmod 600 /absolute/path/to/workspace/.tiangong-research/.env
  ```

Do not place agent login tokens, cloud credentials, or unrelated service keys
in this file.

## Capability declaration example

```json
{
  "schemaVersion": 1,
  "capabilities": [
    {
      "id": "method.public-source",
      "skillPath": "/absolute/path/to/public-source-skill",
      "permissions": ["project-read", "candidate-write", "brokered-network"],
      "allowedHosts": ["api.example.org"],
      "http": {
        "accept": "application/json",
        "allowedContentTypes": ["application/json"],
        "maxResponseBytes": 524288,
        "maxItems": 100
      },
      "coverage": {
        "dimensions": ["research-question"],
        "sourceTypes": ["primary"],
        "fullText": true,
        "publicationDates": true
      },
      "credentials": [
        {
          "id": "source.example.api",
          "allowedHosts": ["api.example.org"],
          "headerName": "Authorization",
          "prefix": "Bearer "
        }
      ]
    }
  ]
}
```

Hosts are exact and may include an explicit port. Wildcards, paths, embedded
credentials, and non-HTTPS targets are rejected. The broker checks every
redirect before sending the next request and never returns credential values in
receipts.
