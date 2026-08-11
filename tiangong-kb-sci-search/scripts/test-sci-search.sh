#!/bin/bash

set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WRAPPER="$SCRIPT_DIR/sci_search.sh"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/tiangong-kb-sci-test.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

FAKE_CLI="$TEST_ROOT/fake-cli"
AUDIT_ARGS="$TEST_ROOT/args.txt"
AUDIT_ENV="$TEST_ROOT/env.txt"
MARKER="$TEST_ROOT/invoked"

printf '%s\n' \
    '#!/bin/bash' \
    'set -euo pipefail' \
    ': > "$FAKE_MARKER"' \
    'printf "%s\n" "$@" > "$FAKE_ARGS"' \
    'printf "SCI=%s\nAI=%s\nTRACK=%s\nEVIL=%s\n" "${TIANGONG_SCI_APIKEY:-}" "${TIANGONG_AI_APIKEY:-}" "${DO_NOT_TRACK:-}" "${EVIL_SETTING:-}" > "$FAKE_ENV"' \
    'printf "{\"ok\":true,\"data\":{\"source\":\"sci\"}}\n"' \
    > "$FAKE_CLI"
chmod 700 "$FAKE_CLI"

run_wrapper() {
    FAKE_ARGS="$AUDIT_ARGS" \
    FAKE_ENV="$AUDIT_ENV" \
    FAKE_MARKER="$MARKER" \
    TIANGONG_AI_CLI_BIN="$FAKE_CLI" \
    "$WRAPPER" "$@"
}

SECRET='owner-only-sci-key-value'
STDOUT="$TEST_ROOT/stdout.txt"
STDERR="$TEST_ROOT/stderr.txt"
TIANGONG_SCI_APIKEY="$SECRET" run_wrapper '{"query":"deterministic query","dry_run":true}' \
    > "$STDOUT" 2> "$STDERR"

grep -Fx 'research' "$AUDIT_ARGS" >/dev/null
grep -Fx 'search' "$AUDIT_ARGS" >/dev/null
grep -Fx -- '--sources' "$AUDIT_ARGS" >/dev/null
grep -Fx 'sci' "$AUDIT_ARGS" >/dev/null
grep -F "SCI=$SECRET" "$AUDIT_ENV" >/dev/null
grep -Fx 'TRACK=1' "$AUDIT_ENV" >/dev/null
if grep -F "$SECRET" "$AUDIT_ARGS" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'secret leaked into argv or command output' >&2
    exit 1
fi

MANAGED_WORKSPACE="$TEST_ROOT/managed-workspace"
MANAGED_NESTED="$MANAGED_WORKSPACE/nested/path"
mkdir -p "$MANAGED_WORKSPACE/.tiangong-research" "$MANAGED_NESTED"
printf '%s\n' '{"schemaVersion":1,"packageName":"@tiangong-ai/cli","packageVersion":"0.0.30"}' \
    > "$MANAGED_WORKSPACE/.tiangong-research/runtime-lock.json"
rm -f "$MARKER"
if (cd "$MANAGED_NESTED" && run_wrapper '{"query":"managed query"}') \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'managed workspace bypassed the Auto Research broker guard' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
grep -F 'AUTO_RESEARCH_BROKER_REQUIRED' "$STDERR" >/dev/null
grep -F '"credentialScope":"broker"' "$STDERR" >/dev/null
grep -F '"networkAttempted":false' "$STDERR" >/dev/null

rm "$MANAGED_WORKSPACE/.tiangong-research/runtime-lock.json"
printf '%s\n' '{"schemaVersion":1,"kind":"tiangong-research-setup-plan","cli":{"package":"@tiangong-ai/cli","version":"9.8.7"},"planSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' \
    > "$MANAGED_WORKSPACE/.tiangong-research/setup-plan.json"
if (cd "$MANAGED_NESTED" && run_wrapper '{"query":"planned managed query"}') \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'plan-only workspace bypassed the Auto Research broker guard' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
grep -F 'AUTO_RESEARCH_BROKER_REQUIRED' "$STDERR" >/dev/null

(cd "$MANAGED_NESTED" && \
    TIANGONG_SCI_APIKEY="$SECRET" \
    run_wrapper '{"query":"isolated query","execution_mode":"standalone","dry_run":true}') \
    > "$STDOUT" 2> "$STDERR"
grep -F 'STANDALONE_MODE_SELECTED' "$STDERR" >/dev/null
grep -F '"executionMode":"standalone"' "$STDERR" >/dev/null
grep -F '"credentialScope":"ambient-or-explicit-owner-env"' "$STDERR" >/dev/null
grep -F '"networkAttempted":false' "$STDERR" >/dev/null
if grep -F "$SECRET" "$AUDIT_ARGS" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'explicit standalone mode leaked a credential' >&2
    exit 1
fi

rm -f "$MARKER"
if run_wrapper "{\"query\":\"blocked\",\"sci_api_key\":\"$SECRET\"}" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'credential-like inline JSON was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
if grep -F "$SECRET" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'rejected inline secret was echoed' >&2
    exit 1
fi

rm -f "$MARKER"
if run_wrapper "{\"query\":\"blocked\",\"sci_url\":\"https://sci.example.test/search?api_key=$SECRET\"}" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'credential-bearing endpoint URL was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
if grep -F "$SECRET" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'rejected endpoint secret was echoed' >&2
    exit 1
fi

REQUEST_FILE="$TEST_ROOT/request.json"
printf '%s\n' "{\"query\":\"blocked\",\"nested\":{\"access_token\":\"$SECRET\"}}" \
    > "$REQUEST_FILE"
rm -f "$MARKER"
if run_wrapper "$(printf '{\"request_file\":\"%s\"}' "$REQUEST_FILE")" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'credential-like request file was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
if grep -F "$SECRET" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'rejected request-file secret was echoed' >&2
    exit 1
fi

MISSING_ENV="$TEST_ROOT/missing.env"
rm -f "$MARKER"
if run_wrapper "$(printf '{\"query\":\"q\",\"env_file\":\"%s\"}' "$MISSING_ENV")" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'missing explicit env file was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]

ENV_FILE="$TEST_ROOT/owner.env"
printf '%s\n' "TIANGONG_SCI_APIKEY=$SECRET" 'EVIL_SETTING=must-not-load' > "$ENV_FILE"
chmod 644 "$ENV_FILE"
rm -f "$MARKER"
if run_wrapper "$(printf '{\"query\":\"q\",\"env_file\":\"%s\"}' "$ENV_FILE")" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'non-owner-only env file was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]

chmod 600 "$ENV_FILE"
unset TIANGONG_SCI_APIKEY TIANGONG_AI_APIKEY EVIL_SETTING || true
run_wrapper "$(printf '{\"query\":\"q\",\"env_file\":\"%s\"}' "$ENV_FILE")" \
    > "$STDOUT" 2> "$STDERR"
grep -F "SCI=$SECRET" "$AUDIT_ENV" >/dev/null
grep -Fx 'EVIL=' "$AUDIT_ENV" >/dev/null
if grep -F "$SECRET" "$AUDIT_ARGS" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'env-file secret leaked into argv or command output' >&2
    exit 1
fi

SAFE_REQUEST="$TEST_ROOT/safe-request.json"
REQUEST_LINK="$TEST_ROOT/request-link.json"
printf '%s\n' '{"query":"safe"}' > "$SAFE_REQUEST"
ln -s "$SAFE_REQUEST" "$REQUEST_LINK"
rm -f "$MARKER"
if run_wrapper "$(printf '{\"request_file\":\"%s\"}' "$REQUEST_LINK")" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'symlinked request file was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]

echo 'tiangong-kb-sci-search wrapper tests passed'
