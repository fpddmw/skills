# Environment Variables

## Required For Database Path (recommended)
- `GDELT_ENV_DB_PATH`: Absolute SQLite DB path used by this skill.

## Required Only For `--classify-mode llm`
- `LLM_API_BASE_URL`: OpenAI-compatible API base URL (example: `https://api.openai.com/v1`)
- `LLM_API_KEY`: API key sent as `Authorization: Bearer <key>`
- `LLM_MODEL`: Model name (example: `gpt-4o-mini`)

## Recommended Config Split
- Keep non-secret defaults in `assets/config.example.json`:
  - `llm_api_base_url`
  - `llm_model`
  - query and time-window settings
- Keep secrets in environment variables only:
  - `LLM_API_KEY`
- Use `assets/config.example.env` as the template for secret-related env vars.

## Optional Runtime Controls
- `HTTPS_PROXY` / `HTTP_PROXY`: Proxy for outbound requests when the runtime network requires proxy routing.
- `NO_PROXY`: Proxy bypass list.

## Notes About GDELT Access
- This skill calls GDELT over HTTP:
  - `https://api.gdeltproject.org/api/v2/doc/doc`
- No GDELT username/password is required for standard DOC API access.

## Runtime Pipeline
- `gdelt_ingest.py ingest`: collect and initial upsert into `gdelt_environment_events`
- `gdelt_enrich.py`: fill URL key + AvgTone/GoldsteinScale + optional classification
- `gdelt_summarize.py`: idempotent upsert into `social_events`
- `gdelt_fetch.py`: compatibility entrypoint (legacy all-in-one CLI)
