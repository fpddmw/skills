# Research environment

## Runtime prerequisites

- Node.js 24.
- `npx` access to `@tiangong-ai/cli@0.0.20`.
- Authenticated `codex` and `claude` executables, unless absolute binary paths
  are selected in `.tiangong-research/config.json`.
- macOS with `/usr/bin/sandbox-exec`, or Linux with Bubblewrap available as
  `bwrap`.

Authenticate the agent CLIs through their standard local login before running a
project. Capability service credentials use the workspace contract below and
are never agent environment variables.

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
